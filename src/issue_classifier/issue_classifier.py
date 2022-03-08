import hashlib
import os
import sys
import logging
import psycopg2
import boto3

from datetime import datetime
from psycopg2.extras import Json

from src.utils.general import read_yaml_file, get_db_conn, get_elasticsearch_conn
from src.pipeline.generate_timesplits import get_time_splits
from src.issue_classifier.feature_creator import FeatureMatrixCreator
from src.issue_classifier.run_model_grid import ModelGrid


class IssueClassifier:
    def __init__(
        self, 
        engine, 
        es_connection,
        metadata_schema, 
        results_schema, 
        features_schema, 
        experiment_config,
        project_folder,
        log_file,
        create_matrices=True,
        matrix_exp_hash=None,
        s3_session=None
        ):
        """
            Experiment for running issue classification
            Args:
                engine: Sql engine
                es_connection: Elasticsearch connection
                metadata_schema: Schema name for metadata tables
                results_schema: Schema name for results tables
                features_schema: Schema name with features tables (depricated)
                experiment_config: Configuration of the experiment
                project_folder: Folder where the experiment components (matrices, models) will be saved
                log_file: File path of the log file
                create_matrices: Flag to indicate whether the matrix should be created or whether they already exist
                matrix_exp_hash: The experiment hash from which the matrices are reused. If create_matrices is False, this can't be None. 
        """

        self.engine = engine
        self.es = es_connection
        self.metadata_schema = metadata_schema
        self.results_schema = results_schema
        self.features_schema = features_schema
        self.config = experiment_config
        self.experiment_hash = self._get_experiment_hash()
        self.project_folder = project_folder
        # self.s3_creds = s3_creds
        self.s3_session = s3_session

        # Handling the S3 vs disk folder
        if self.project_folder[:3] == 's3:':
            if s3_session is None: 
                raise ValueError('Need to provide boto3 session if the project folder is an S3 bucket')
        else:
            if not os.path.isdir(self.project_folder):
                os.mkdir(self.project_folder)

        self.time_splits = get_time_splits(
            temporal_config = self.config['temporal_config'],
            visualize=False,
            # figure_path=os.path.join(self.project_folder, 'timechops.png')
        )

        self.start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_file = log_file
        
        self.create_matrices = create_matrices
        self.matrix_exp_hash = matrix_exp_hash

        if (not create_matrices) and (self.matrix_exp_hash is None):
            raise ValueError('If the matrices are not created, the experiment hash for the relevant matrices should be provided')

        self.matrix_creator = None
        self.modeller = None

    def _get_experiment_hash(self):
        """ Create the MD5 hash of the experiment"""
        s = str(self.config)

        hash_object = hashlib.md5(s.encode())
        hex_hash = hash_object.hexdigest()

        return hex_hash

    def _run_matrix_creation(self):
        """ Create and store matrices """

        # Initializing the matrix creator
        self.matrix_creator = FeatureMatrixCreator(
            engine=self.engine,
            es_connection=self.es,
            cohort_config=self.config['cohort_config'],
            temporal_config=self.config['temporal_config'],
            features_config=self.config['features'],
            preprocessing_config=self.config['preprocessing_config'],
            es_config=self.config['es_config'],
            experiment_hash=self.experiment_hash,
            project_folder=self.project_folder,
            s3_session=self.s3_session
        )

        logging.info('Running feature matrix generation')
        self.matrix_creator.generate_text_features()

    def _run_model_grid(self):
        """ Train/evaluate the model grid. 
            The matrix creation needs to be completed before running the model grid
        """
        if self.create_matrices:
            mat_uuids = self.matrix_creator.matrix_uuids
        else:
            mat_uuids = self.fetch_matrix_pairs_experiment(exp_hash=self.matrix_exp_hash)

        print(mat_uuids)

        # run model grid
        mod_grid = ModelGrid(
            engine=self.engine,
            metadata_schema=self.metadata_schema, 
            results_schema=self.results_schema, 
            features_schema=self.features_schema,
            exp_hash=self.experiment_hash,
            grid_config=self.config['grid_config'],
            issue_areas=self.config['issue_areas'],
            matrix_uuids=mat_uuids,
            project_folder = self.project_folder,
            s3_session=self.s3_session
        )

        logging.info('Running models')
        mod_grid.run()


    def _write_to_experiments(self):
        """Add the experiment details to the experiments table"""

        # Checking whether the hash aready exists
        cursor = self.engine.cursor()
        q = """ 
            select * from {}.experiments where experiment_hash='{}'
            """.format(self.metadata_schema, self.experiment_hash)

        try:
            cursor.execute(q)
            cursor.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            raise
        
        # The entry is added only if doen't exist
        if cursor.rowcount == 0:
            q = """
                    insert into {}.experiments 
                        (experiment_hash, config, time_splits)
                    values (%s, %s, %s)
                """.format(
                    self.metadata_schema
                )

            var = (
                self.experiment_hash,
                Json(self.config),
                len(self.time_splits)
            )
            
            try:
                cursor.execute(q, var)
                self.engine.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                logging.error(error)
                raise

    def _write_to_experiment_runs(self, status):
        """Add run entry to the experiment runs table"""
        q = """
                insert into {}.experiment_runs
                    (experiment_hash, start_time, run_status, project_folder, log_location)
                values
                    (%s, %s, %s, %s, %s)
            """.format(
                self.metadata_schema
            )
        
        var = (
            self.experiment_hash,
            self.start_time,
            status,
            self.project_folder,
            self.log_file
        )

        cursor = self.engine.cursor()
        try:
            cursor.execute(q, var)
            self.engine.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            raise psycopg2.DatabaseError

    def _update_run_status(self, status):
        """update the run status in the experiment_runs table"""
        q = """
                update {0}.experiment_runs
                    set run_status=%s
                where experiment_hash='{1}' and start_time='{2}' 
            """.format(
                    self.metadata_schema,
                    self.experiment_hash,
                    self.start_time
                )
        
        cursor = self.engine.cursor()

        try:
            cursor.execute(q, (status,))
            self.engine.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            raise psycopg2.DatabaseError
        
    # TODO
    def validate(self):
        """ Validate the config file"""
        pass

    # TODO: Set up the experiment class to set a replace=False
    def fetch_matrix_pairs_experiment(self, exp_hash):
        """fetch the matrix pairs from a previous experiment"""

        q = """
                select 
                    max(case when matrix_type='train' then matrix_uuid end) as train_mat,
                    max(case when matrix_type='test' then matrix_uuid end) as test_mat
                from {}.matrices where built_by_experiment='{}' 
                group by time_split_index;
            """.format(self.metadata_schema, exp_hash)

        cursor = self.engine.cursor()

        try:
            cursor.execute(q)
            res = cursor.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            raise psycopg2.DatabaseError

        # creating the list of dictionaries

        matrix_uuids = [{'train': x[0], 'test': x[1]} for x in res]

        return matrix_uuids


    def run(self):
        """ Run the pipeline """

        # Add experiment metadata entries
        self._write_to_experiments()
        self._write_to_experiment_runs(status='started')

        try:
            # Matrix creation
            if self.create_matrices:
                self._run_matrix_creation()
            
            # run model grid
            self._run_model_grid()
            self._update_run_status('completed')

        except Exception as error:
            self._update_run_status('failed')
            logging.error(error)
            raise              
        