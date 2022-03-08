import os
import yaml
import importlib
import itertools as it
import logging
import json
import hashlib
import pandas as pd
import ohio.ext.pandas
import joblib
import boto3
import tempfile

from typing import List, Dict
from io import BytesIO

import src.utils.project_constants as constants

from src.utils.general import read_yaml_file, get_db_conn, format_s3_path
from src.issue_classifier.evaluation_functions import get_model_predictions, write_to_predictions
from src.utils.modeling import parse_sparse_bow_json

logging.basicConfig(level=logging.DEBUG)


class ModelGrid:
    def __init__(self, 
        engine, 
        metadata_schema, 
        results_schema, 
        features_schema,
        exp_hash, 
        grid_config,
        issue_areas, 
        matrix_uuids: List[Dict],
        project_folder: str,
        s3_session=None,
        ):
        """
            The class that runs the model grid for the issue area classifier
            args:
                engine          : sql connection
                metadata_schema : Schema that has the modeling metadata
                results_schema  : Schema that stores the train and test predictions
                features_schema : Schema that stores the features and time blocks
                exp_hash        : Hash of the experiment
                grid_config     : The model grid as defined in the configuration file
                issue_areas     : Issue areas to classify. A list
                matrix_uuids    : A list of dictionaries that has the train and test matrix uuids for each time split. 
                                  Each dictionary should have a 'train' and a 'test' key
                project_folder   : The folder in disk or S3 where the project matrices and 
        """
        self.sql_engine = engine
        self.metadata_schema = metadata_schema
        self.results_schema = results_schema
        self.features_schema = features_schema
        self.exp_hash = exp_hash
        self.model_grid = self._parse_model_grid(grid_config)
        self.issue_areas = issue_areas
        self.matrix_uuids = matrix_uuids 
        self.project_folder = project_folder
        self.s3_session = s3_session

        if self.project_folder[:3] =='s3:':
            if s3_session is None:
                raise ValueError('A boto3 session should be provided if the project path is a S3 bucket')
            
            self.models_folder = '{}/{}'.format(self.project_folder, 'models')
            self.matrices_folder = '{}/{}'.format(self.project_folder, 'matrices')
            # TODO: It would be good to have a check to see if any matrices are stored in the folder. 
            # Might be overkill

        else:
            self.models_folder = os.path.join(self.project_folder, 'models')
            if not os.path.isdir(self.models_folder):
                os.mkdir(self.models_folder)
            
            self.matrices_folder = os.path.join(self.project_folder, 'matrices')
            if not os.path.isdir(self.matrices_folder):
                raise ValueError('Matrices folder does not exist')

    def _parse_model_grid(self, grid_config):
        """ 
            parse the model grid into a dictionary of all different model groups 
            args:
                grid_config: Grid configuration from the config file
            return:
                dictionary of the model grid Dict[List[Dict]]           
        """

        model_grid = dict()
        for mod, mod_grid in grid_config.items():
            all_params = sorted(mod_grid)
            
            # All the different combinations
            combs = it.product(*(mod_grid[name] for name in all_params))
            params = [dict(zip(all_params, x)) for x in combs]
        
            model_grid[mod] = params

        return model_grid

    def _import_model_from_str(self, mod_str):
        """ return a model object from the model string """

        # Splitting the module and the model class from the model type string
        m, c = mod_str.rsplit('.', 1)

        # importing the module
        p = importlib.import_module(m)

        # fetching the model class
        mod_obj = getattr(p, c) 

        return mod_obj

    def _write_to_model_groups(self, db_cursor, model_type, hyperparameters):
        """ write a model group to the table 
            args:
                db_cursor: Cursor object of psycopg
                model_type: Type of the model (string)
                hyperparamets: Model hyperparameters (dictionary)

            return: 
                the model_group_id that was added
        """
        # TODO: This writes the model groups for each experiment, 
        # even if the ecact model group is already there. It is suboptimal. Sould keep only unique model groups

        hyperparameters = json.dumps(hyperparameters)

        q = """
               INSERT INTO  {}.model_groups 
                (model_type, hyperparameters, experiment_hash)
               VALUES ({});
            """.format(self.metadata_schema, ', '.join(['%s'] * 3))

        var = (
            model_type,
            hyperparameters,
            self.exp_hash
        )

        db_cursor.execute(q, var)

        # Fetching the model_group_id for the written row
        # TODO: This is hacky. Improve
        q = f"select max(model_group_id) from {self.metadata_schema}.model_groups"
        db_cursor.execute(q)

        mod_grp_id = db_cursor.fetchone()
        return mod_grp_id[0]
        

    def _create_model_hash(self, model_type, train_mat_uuid, hyperparameters, issue_area):
        """ create a hash for the model given the model type, train matrix, hyperparameters and the experiment"""
        s = model_type + train_mat_uuid + str(hyperparameters) + issue_area

        hash_object = hashlib.md5(s.encode())
        hex_hash = hash_object.hexdigest()

        return hex_hash

    def _write_to_models(self, db_cursor, mod_group_id, model_hash, train_matrix_uuid, issue_area):
        """ write the model info to the models table """

        q = """
               INSERT INTO {}.models 
                (model_hash, model_group_id, built_by_experiment, train_matrix_uuid, issue_area)
               VALUES ({});
            """.format(
                self.metadata_schema, 
                ', '.join(['%s'] * 5)
            )

        var = (
            model_hash,
            mod_group_id,
            self.exp_hash,
            train_matrix_uuid,
            issue_area
        )

        db_cursor.execute(q, var)


        # Fetching the model_id for the written row
        # TODO: This is hacky. Improve
        q = f"select max(model_id) from {self.metadata_schema}.models"
        db_cursor.execute(q)
        modid = db_cursor.fetchone()
        
        return modid[0]

    def _load_feature_matrix(self, matrix_uuid, issue_area, feature_mat_format='sparse'):
        """ 
            fetch the feature matrix for the uuid and the relevant label for the issue area
            The matrix should be already saved on the disk. 
        """
        
        if self.s3_session is not None:
            logging.info('loading the matrix from S3')
            s3 = self.s3_session.resource('s3')

            s3_bucket = constants.S3_BUCKET
            mat_folder = self.matrices_folder.lstrip('s3://{}/'.format(s3_bucket))
            # Stripping the s3://<s3_bucket>/ 
            mat_folder = format_s3_path(self.matrices_folder)
            if feature_mat_format == 'sparse':
                fkey = '{}/{}.json'.format(mat_folder, matrix_uuid) 
                df = parse_sparse_bow_json(json_file=fkey)
            else:
                fkey = '{}/{}.csv'.format(mat_folder, matrix_uuid)
                content = s3.Object(s3_bucket, fkey).get()['Body'].read()
                df = pd.read_csv(BytesIO(content))
        else:
            try:
                mat_path = os.path.join(self.matrices_folder, f'{matrix_uuid}.csv')
                readers = pd.read_csv(mat_path, chunksize=10**6)
            except FileNotFoundError:
                raise FileNotFoundError('Matrix {} not found in the matrices folder'.format(matrix_uuid))
            
            df = pd.concat([x for x in readers], ignore_index=True)

        # Should contain the columns, 'entity_id', and 'as_of_date'
        df.set_index(['entity_id', 'as_of_date'], inplace=True)

        # labels
        logging.info('Extracting relevant label for {}'.format(issue_area))
        label_columns = [x for x in df.columns if '_label' in x]
        labels = df[label_columns]
        relevant_label = labels[f'{issue_area}_label']

        # drop labels
        df.drop(label_columns, axis=1, inplace=True)

        return df, relevant_label

    def _store_model(self, model_obj, model_hash):
        """write the model to the disk or S3 bucket"""

        # If the project path is an S3 bucket
        if self.s3_session is not None:
            # removing the S3://<bucket-name>/ from the folder name
            model_folder = format_s3_path(self.models_folder)
            s3 = self.s3_session.resource('s3')
            s3_bucket = constants.S3_BUCKET
            fkey = '{}/{}'.format(model_folder, model_hash)  

            logging.info('Storing the trained model at {}'.format(fkey))
            with tempfile.TemporaryFile() as fp:
                joblib.dump(model_obj, fp)
                fp.seek(0) # bringing the cursor back to the top of the file
                s3.Bucket(s3_bucket).put_object(Key=fkey, Body=fp.read())
        else:
            save_target = os.path.join(self.models_folder, model_hash)
            logging.info('Storing the trained model at {}'.format(save_target))
            joblib.dump(model_obj, save_target)
       
    def run_model_grid_issue_area(self, issue_area):
        """
            Run the model grid specified for a single issue area
                1. Trains the models for all model groups
                2. Update model_groups, and models tables
                3. Write the train and test results to the train/test matrices
        """
        logging.info('Running model grid for issue area {}'.format(issue_area))

        cur = self.sql_engine.cursor()
        no_time_splits = len(self.matrix_uuids)

        # Iterate over all model types
        for mod_type, mod_groups in self.model_grid.items():
            mod_class = self._import_model_from_str(mod_type)
            
            # Iterate over all model configs in the model type (model groups)
            for hp in mod_groups:
                mod_group_id = self._write_to_model_groups(cur, mod_type, hp)

                logging.info('Processing the model group {} , model type {}, hyperparameter {}'.format(
                        mod_group_id, 
                        mod_type, 
                        hp
                    )
                )
            
                # Iterating over the timesplits
                for i in range(no_time_splits):
                    logging.info('Processing time chop idx: {}'.format(i))

                    # Fetching uuids
                    train_uuid = self.matrix_uuids[i]['train']
                    test_uuid = self.matrix_uuids[i]['test']

                    # creating model hash
                    mod_hash = self._create_model_hash(mod_type, train_uuid, hp, issue_area)
                    logging.info('Writing the model {} to the DB'.format(mod_hash))
                    
                    mod_id = self._write_to_models(
                        cur, 
                        mod_group_id, 
                        mod_hash, 
                        train_uuid, 
                        issue_area
                    )

                    # matrices
                    logging.info('loading the train matrix {}'.format(train_uuid))
                    train_mat, train_labels = self._load_feature_matrix(train_uuid, issue_area)
                    
                    # model object
                    model = mod_class(**hp)
                    
                    # training the model
                    logging.info('Training the model id {}'.format(mod_id))
                    model.fit(train_mat.values, train_labels)
                    
                    # Save the model
                    logging.info('Saving the model {} to disk'.format(mod_hash))
                    self._store_model(model, mod_hash)

                    # generate train predictions
                    logging.info(
                        'generating predictions for the train set {} using model {}'.format(
                            train_uuid,
                            mod_id
                        )
                    )
                    train_preds = get_model_predictions(model, train_mat)    

                    logging.info('Writing predictions to the DB')
                    # Write to predictions table
                    write_to_predictions(
                        engine=self.sql_engine,
                        predictions=train_preds,
                        model_id=mod_id,
                        matrix_uuid=train_uuid,
                        experiment_hash=self.exp_hash,
                        label_values=train_labels,
                        issue_area=issue_area,
                        schema=self.results_schema,
                        table='train_predictions'
                    )

                    # generate test predictions
                    test_mat, test_labels = self._load_feature_matrix(test_uuid, issue_area)
                    logging.info(
                        'generating predictions for the test set {} using model {}'.format(
                            test_uuid,
                            mod_id
                        )
                    )
                    test_preds = get_model_predictions(model, test_mat)

                    logging.info('Writing predictions to the DB')
                    # Write to predictions table
                    write_to_predictions(
                        engine=self.sql_engine,
                        predictions=test_preds,
                        model_id=mod_id,
                        matrix_uuid=test_uuid,
                        experiment_hash=self.exp_hash,
                        label_values=test_labels,
                        issue_area=issue_area,
                        schema=self.results_schema,
                        table='test_predictions'
                    )
                    # TODO:
                    # 4. Evaluate model
                    # 5. Write evaluations to DB      
                    self.sql_engine.commit()


    def run(self):
        """ run the complete model grid """

        for issue in self.issue_areas:
            logging.info('Classifying issue area {}'.format(issue))
            self.run_model_grid_issue_area(issue)        

