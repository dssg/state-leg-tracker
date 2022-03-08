import os
import pandas as pd
import logging
import psycopg2
import uuid
import boto3
import hashlib
import json
import tempfile
import joblib

from sklearn.feature_extraction.text import TfidfVectorizer
from psycopg2.extras import execute_batch, Json
from io import StringIO, BytesIO

import src.utils.project_constants as constants

from src.pipeline.text_preprocessing import run_preprocessing_steps
from src.pipeline.generate_timesplits import get_time_splits
from src.pipeline.tf_id_features import tf_idf_features

from src.utils.general import get_db_conn, read_yaml_file, get_elasticsearch_conn, format_s3_path


class FeatureMatrixCreator:
    def __init__(
        self,
        engine,
        es_connection,
        cohort_config,
        temporal_config,
        features_config,
        preprocessing_config,
        es_config,
        experiment_hash,
        project_folder,
        s3_session=None
    ):
        """
            Creating and storing the feature matrices
            Args:
                engine: sql enigine
                es_connection: Elasticsearch connection
                cohort_config: Cohort configuration from the experiment config (Dict)
                temporal_config: Time split configuration (Dict)
                features_config: Configuration for creating text features (Dict)
                preprocessing_config: Configuratiion for cleaning the text for feature creation (Dict)
                experiment_hash: Hash for the experiment (str)
                project_folder: The target folder for saving the project components. 
                                A 'matrices' folder will be created in the project folder
        """
        self.sql_engine = engine
        self.es = es_connection
        self.cohort_query = cohort_config['query']
        self.temporal_config = temporal_config
        self.features_config = features_config
        self.experment_hash = experiment_hash
        self.preprocessing_steps = preprocessing_config
        self.es_config = es_config
        self.matrix_uuids = list()
        self.s3_session = s3_session

        if project_folder[:3] == 's3:':           
            self.martrix_folder = '{}/{}'.format(project_folder, 'matrices')
            self.feature_models_folder = '{}/{}'.format(project_folder, 'feature_models')
   
            if self.s3_session is None:
                raise ValueError('Need to provide a boto3 session if the project folder S3')
        else:            
            self.martrix_folder = os.path.join(project_folder, 'matrices')
            if not os.path.isdir(self.martrix_folder):
                os.mkdir(self.martrix_folder)

            self.feature_models_folder = os.path.join(project_folder, 'feature_models')
            if not os.path.isdir(self.martrix_folder):
                os.mkdir(self.feature_models_folder)

    def _prepare_matrix(self, as_of_dates, n_jobs=-1):
        """ Prepares the cohort, labels and text for feature creation
            1. fetches cohort
            2. fetches labels and relevant doc_ids
            3. Fetches text from elastic search
            4. Runs the preprocessing steps specified in the config
            
            args:
                as_of_dates: List of as_of_dates included in the matrix
            return:
                A dataframe with 
                    ids, as_of_date, labels, text, preprocessed text 
                matrix uuid

        """

        ids_and_labels = self._get_cohort(as_of_dates)
        ids = ids_and_labels[['bill_id', 'doc_id']].to_dict('records')

        texts = self._retrieve_text_from_es(
            ids, 
            query_size=self.es_config['query_size'], 
            text_key=self.es_config['text_field']
        )

        # Labels, entity_id, and texts
        master_mat = ids_and_labels.merge(texts, on=['bill_id', 'doc_id'], how='inner')
        master_mat.rename(columns={'bill_id': 'entity_id'}, inplace=True)

        # set index to bill_id, and as_of_date
        master_mat.set_index(['entity_id', 'as_of_date'], inplace=True)

        preprocessed = run_preprocessing_steps(
            texts=master_mat['text'], 
            processing_steps=self.preprocessing_steps,
            n_jobs=n_jobs
        )

        master_mat = master_mat.join(preprocessed) 

        mat_uuid = uuid.uuid4().hex

        # separating labels
        label_columns = [x for x in ids_and_labels.columns if '_label' in x]
        labels = master_mat[label_columns]
        master_mat.drop(label_columns, axis=1, inplace=True)

        return master_mat, mat_uuid, labels

    def _create_feature_model_hash(self, model_type, train_matrix_uuid, hyperparameters):
        """ create the hash for text feature creator model """
        s = model_type + train_matrix_uuid + str(hyperparameters)  

        hash_object = hashlib.md5(s.encode())
        hex_hash = hash_object.hexdigest()

        return hex_hash  

    def _write_to_text_feature_creators(self, mod_group_id, model_hash, train_matrix_uuid):
        """ write the model info to the models table """

        q = """
               INSERT INTO issue_classifier_metadata.text_feature_creators 
                (feature_creator_hash, text_feature_group_id, built_by_experiment, train_matrix_uuid)
               VALUES ({});--3
            """.format(', '.join(['%s'] * 4))

        db_cursor = self.sql_engine.cursor()
        
        var = (
            model_hash,
            mod_group_id,
            self.experment_hash,
            train_matrix_uuid
        )

        db_cursor.execute(q, var)
        self.sql_engine.commit()

        # Fetching the model_id for the written row
        # TODO: This is hacky. Improve
        q = "select max(feature_creator_id) from issue_classifier_metadata.text_feature_creators"
        db_cursor.execute(q)
        modid = db_cursor.fetchone()
        
        return modid[0]        

    def _write_to_text_feature_groups(self, model_type, hyperparameters):
        """ write a feature group model group to the table 
            args:
                db_cursor: Cursor object of psycopg
                model_type: Type of the model (string)
                hyperparamets: Model hyperparameters (dictionary)

            return: 
                the model_group_id that was added
        """

        # Keeping the serializable hyperparameters
        hp = {k:v for k, v in hyperparameters.items() if (v is not None) and (k is not 'dtype') }
        hp = json.dumps(hp)

        # Checking whether the model group already exists
        q = """
            SELECT 
                text_feature_group_id 
            from issue_classifier_metadata.text_feature_groups
            where feature_type='{}' and hyperparameters='{}'
        """.format(model_type, hp)

        db_cursor = self.sql_engine.cursor()
        db_cursor.execute(q)

        mod_grp_id = db_cursor.fetchone()

        if mod_grp_id is None:
            q = """
                INSERT INTO  issue_classifier_metadata.text_feature_groups 
                    (feature_type, hyperparameters, experiment_hash)
                VALUES ({});
                """.format(', '.join(['%s'] * 3))

            var = (
                model_type,
                hp,
                self.experment_hash
            )

            db_cursor.execute(q, var)
            self.sql_engine.commit()

            # Fetching the model_group_id for the written row
            # TODO: This is hacky. Improve
            q = "select max(text_feature_group_id) from issue_classifier_metadata.text_feature_groups"
            db_cursor.execute(q)

            mod_grp_id = db_cursor.fetchone()
        else:
            logging.info('The model group already exists as feature_group_id {}'.format(mod_grp_id[0]))

        return mod_grp_id[0]
           
    def generate_text_features(self, feature_matrix_storage_format='sparse'):
        """ Generate text features for the issue classifier 
            1. Creates train and test feature matrices for all time chops, 
            2. writes it to the db
            3. saves the matrices in the project folder
        """

        time_splits = get_time_splits(
            temporal_config = self.temporal_config,
            visualize=False,
            figure_path=os.path.join(self.martrix_folder, 'timechop.png')
        )

        feature_type = self.features_config['type']
        hp = self.features_config.get('hyperparameters') # This could be null

        for i, time_chop in enumerate(time_splits):
            train_as_of_dates = time_chop['train_matrix']['as_of_times']
            test_as_of_dates = time_chop['test_matrices'][0]['as_of_times']

            # logging.info(train_as_of_dates)
            # logging.info(test_as_of_dates)

            # train 
            logging.info('Fetching text data for train matrix')
            train_data, train_uuid, train_labels = self._prepare_matrix(train_as_of_dates)

            logging.info('Fetching text data for test matrix')
            test_data, test_uuid, test_labels = self._prepare_matrix(test_as_of_dates)

            # Keeping track of uuids
            self.matrix_uuids.append({'train': train_uuid, 'test': test_uuid})

            # TODO: This only handles TF-IDF. Handle other features as well
            # Features
            if hp is not None:
                tf = TfidfVectorizer(**hp)
            else:
                tf = TfidfVectorizer()

            # Training the model
            tf.fit(train_data['preprocessed'].tolist())

            model_hp = tf.get_params()
            feature_model_hash = self._create_feature_model_hash(feature_type, train_uuid, model_hp)

            # Writing to the model group
            feature_group_id = self._write_to_text_feature_groups(
                model_type=feature_type,
                hyperparameters=model_hp
            )

            # Writing to the models
            creator_id = self._write_to_text_feature_creators(
                mod_group_id=feature_group_id,
                model_hash=feature_model_hash,
                train_matrix_uuid=train_uuid
            )

            # Storing the trained feature model
            self._store_feature_model(
                model_object=tf,
                model_hash=feature_model_hash
            )

            logging.info('extracting the train sparse matrix')
            train_features_csr = tf.transform(train_data['preprocessed'].tolist())

            logging.info('extracting the test sparse matrix')
            test_features_csr = tf.transform(test_data['preprocessed'].tolist())

            logging.info('Beginning to store the feature matrices')
            if feature_matrix_storage_format=='sparse':
                self._store_bow_sparse_matrix(
                    matrix=train_features_csr, 
                    vocabulary=tf.vocabulary_, 
                    indexes=train_data.index,
                    matrix_uuid=train_uuid
                )

                self._store_bow_sparse_matrix(
                    matrix=test_features_csr, 
                    vocabulary=tf.vocabulary_, 
                    indexes=test_data.index,
                    matrix_uuid=test_uuid
                )
            else:
                logging.warning('Storing the feature matrix as dense. Can cause memory issues')
                train_features_dense = pd.DataFrame(
                    train_features_csr.todense(), 
                    columns=tf.get_feature_names(),
                    index=train_data.index
                )
                test_features_dense = pd.DataFrame(
                    test_features_csr.todense(), 
                    columns=tf.get_feature_names(),
                    index=test_data.index
                )

                # add labels
                train_features_dense = train_features_dense.join(train_labels)
                test_features_dense = test_features_dense.join(test_labels)

                # Bill id and as_of_date as columns
                train_features_dense.reset_index(inplace=True)
                test_features_dense.reset_index(inplace=True)

                logging.info('Writing matrix {} to {}'.format(train_uuid, self.martrix_folder))
                self._store_dense_feature_matrix(matrix=train_features_dense, matrix_uuid=train_uuid)

                logging.info('Writing matrix {} to {}'.format(test_uuid, self.martrix_folder))
                self._store_dense_feature_matrix(matrix=test_features_dense, matrix_uuid=test_uuid)
        
            logging.info('Writing matrix metadata to the db')
            self._write_to_matrices(
                matrix_type='train',
                matrix_uuid=train_uuid,
                as_of_dates=train_as_of_dates,
                time_split_index=i,
                feature_creator_id=creator_id,
                stored_file_format=feature_matrix_storage_format
            )
            self._write_to_matrices(
                matrix_type='test',
                matrix_uuid=test_uuid,
                as_of_dates=test_as_of_dates,
                time_split_index=i,
                feature_creator_id=creator_id,
                stored_file_format=feature_matrix_storage_format
            )

        return None

    def _store_feature_model(self, model_object, model_hash):
        """Storing the trained text feature creator groups"""

        # S3 bucket
        if self.s3_session is not None:
            feature_model_folder = format_s3_path(self.feature_models_folder)

            s3 = self.s3_session.resource('s3')
            s3_bucket = constants.S3_BUCKET

            fkey = '{}/{}'.format(feature_model_folder, model_hash)

            logging.info('Storing the trained feauture creator at {}'.format(fkey))

            with tempfile.TemporaryFile() as fp:
                joblib.dump(model_object, fp)
                fp.seek(0)

                s3.Bucket(s3_bucket).put_object(Key=fkey, Body=fp.read())
        # Disk
        else:
            save_target = os.path.join(self.feature_models_folder, model_hash)
            logging.info('Storing the trained feauture creator at {}'.format(save_target))

            joblib.dump(model_object, save_target)

    def _store_bow_sparse_matrix(self, matrix, vocabulary, indexes, matrix_uuid):
        """ Store the feature matrix as a compressed sparse matrix. Combining relevant info to a dictonary and saving as a JSON
            Using JSON to preserve the readability. Might not be the most efficient way to store this information
        
        Args:
            matrix (sparse matrix): The sparse matrix that representes the BoW
            vocabulary (Dict): vocabulary mapping to the columns indices of the sparse mat)
            indexes (pd.Index): The list of indexes (entity_id, as_of_date) that will map to the row indices of the sparse matrix
            matrix_uuid: The uuid used for governance
        """

        # creating the mapping between the integer indexes and the entity_ids
        # Mapping it as a dict to keep the mappings consistent with the vocabulary
        id_mapping = {(str(x[0]), x[1].strftime('%Y-%m-%d')): i for i, x in enumerate(indexes)}

        # We convert the sparse matrix to a dictionary to make it JSON serializable. 
        # converting the scipy sparse matrix to a dictionary of keys
        mat = matrix.todok()

        # Converting the tuple keys to string to enable JSON
        mat = {', '.join((str(k[0]), str(k[1]))): v for k, v in mat.items()}
        id_mapping = {', '.join(k): v for k, v in id_mapping.items()}

        d = dict()
        d['matrix'] = mat
        d['vocabulary'] = vocabulary
        d['id_mapping'] = id_mapping

        fn = '{}.json'.format(matrix_uuid)
        s3_bucket = constants.S3_BUCKET

        if self.s3_session is not None:
            # removing the S3://<bucket-name>/ from the folder name
            mat_folder = format_s3_path(self.martrix_folder)
            fkey = '{}/{}'.format(mat_folder, fn)
            s3 = self.s3_session.resource('s3')

            logging.info('Storing the matrix {} at {}'.format(matrix_uuid, fkey))
            # s3.upload_fileobj(json.dumps(d), s3_bucket, fkey)
            s3.Bucket(s3_bucket).put_object(Key=fkey, Body=json.dumps(d))
        else:
            mat_path = os.path.join(self.martrix_folder, fn)
            logging.info('Storing the matrix {} at {}'.format(matrix_uuid, mat_path))

            with open(mat_path, 'w') as f:
                json.dump(d, f)

    def _store_dense_feature_matrix(self, matrix, matrix_uuid):
        """ Storing a dense feature matrix as a csv in an S3 bucket/Disk """
        
        fn = '{}.csv'.format(matrix_uuid)
        
        # If the project folder is a S3 bucket
        if self.s3_session is not None:
            # removing the S3://<bucket-name>/ from the folder name
            mat_folder = format_s3_path(self.martrix_folder)
            
            s3_bucket = constants.S3_BUCKET
            fkey = '{}/{}'.format(mat_folder, fn)

            csv_buffer = StringIO()
            matrix.to_csv(csv_buffer, index=False)

            logging.info('Storing the matrix in the S3 bucket')
            buffer2 = BytesIO(csv_buffer.getvalue().encode())
            s3 = self.s3_session.client('s3')      
            s3.upload_fileobj(buffer2, s3_bucket, fkey)
        else:
            matrix.to_csv(
                os.path.join(self.martrix_folder, fn), 
                index=False
            )

    def _write_to_matrices(self, matrix_type, matrix_uuid, as_of_dates, time_split_index, feature_creator_id, stored_file_format):
        """ add matrix entry to the database """

        start_aod = as_of_dates[0]
        end_aod = as_of_dates[-1]

        if matrix_type == 'train':
            as_of_date_frequency = self.temporal_config['training_as_of_date_frequencies']
        else:
            as_of_date_frequency = self.temporal_config['test_as_of_date_frequencies']

        matrix_id = '{}_{}'.format(start_aod, end_aod)

        # TODO: This is wrong
        lookback_duration = end_aod - start_aod
        
        feature_start_time = str(start_aod)
        indices = ["entity_id", "as_of_date"]

        as_of_times = [str(element) for element in as_of_dates]
        matrix_metadata = {
            'as_of_date_frequency': as_of_date_frequency,
            'as_of_times': as_of_times,
            'end_time': str(end_aod),
            'feature_start_time': feature_start_time,
            'matrix_id': matrix_id,
            'indices': indices,
            'matrix_info_end_time': str(end_aod),
            'matrix_type': matrix_type,
            'max_training_history': self.temporal_config['max_training_histories'],
            'training_as_of_date_frequency': self.temporal_config["training_as_of_date_frequencies"],
            'training_label_timespan': self.temporal_config["label_timespans"]
        }

        q = "insert into issue_classifier_metadata.matrices VALUES({})".format(", ".join(['%s'] * 10))
        var = (
            matrix_id,
            matrix_uuid,
            matrix_type,
            feature_start_time,
            lookback_duration,
            Json(matrix_metadata),
            self.experment_hash,
            time_split_index,
            feature_creator_id,
            stored_file_format
        )

        cursor = self.sql_engine.cursor()

        try:
            cursor.execute(q, var)
            self.sql_engine.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)

    def _get_cohort(self, as_of_dates):
        """ Fetch the set of bill ids and doc ids for a set of as_of_dates """

        cohort = pd.DataFrame()
        for aod in as_of_dates:
            bill_ids = self._get_bill_ids_for_aod(aod)

            docs_labels = self._fetch_doc_ids_labels(bill_ids, aod)
            cohort = cohort.append(docs_labels, ignore_index=True)

        return cohort

    def _get_bill_ids_for_aod(self, as_of_date):
        """ get the bill ids for an as of date """
        cursor = self.sql_engine.cursor()
        q = self.cohort_query.format(as_of_date=as_of_date)
        try:
            cursor.execute(q)
            
            bill_ids = cursor.fetchall()
            bill_ids = [x[0] for x in bill_ids]
            
            self.sql_engine.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)

        return bill_ids

    def _fetch_doc_ids_labels(self, bill_ids, as_of_date):
        """ Fetch the relevant doc_id for the given cohort"""
        cursor = self.sql_engine.cursor()
        q = """ 
                with all_bills as( 
                    select bill_id, max(doc_id) as doc_id
                    from clean.bill_docs
                    where bill_id in {} and doc_date < '{}'
                    group by bill_id
                ),
                reproductive_rights as(
                    select bill_id, doc_id, max(relevance_score), reproductive_rights 
                    from labels_es.reproductive_rights
                    group by bill_id, doc_id, reproductive_rights
                ), 
                criminal_law as(
                    select bill_id, doc_id, max(relevance_score), criminal_law_reform
                    from labels_es.criminal_law_reform
                    group by bill_id, doc_id, criminal_law_reform
                ),
                immigrant_rights as(
                    select bill_id, doc_id, max(relevance_score), immigrant_rights
                    from labels_es.immigrant_rights
                    group by bill_id, doc_id, immigrant_rights
                ),
                lgbt_rights as(
                    select bill_id, doc_id, max(relevance_score), lgbt_rights
                    from labels_es.lgbt_rights
                    group by bill_id, doc_id, lgbt_rights
                ),
                racial_justice as(
                    select bill_id, doc_id, max(relevance_score), racial_justice
                    from labels_es.racial_justice
                    group by bill_id, doc_id, racial_justice
                ),
                voting_rights as(
                        select bill_id, doc_id, max(relevance_score), voting_rights
                        from labels_es.voting_rights
                        group by bill_id, doc_id, voting_rights
                )
                select 
                    bill_id, 
                    doc_id, 
                    coalesce(reproductive_rights, 0) as reproductive_rights_label, 
                    coalesce(criminal_law_reform, 0) as criminal_law_reform_label, 
                    coalesce(immigrant_rights, 0) as immigrant_rights_label, 
                    coalesce(lgbt_rights, 0) as lgbt_rights_label, 
                    coalesce(racial_justice, 0) as racial_justice_label, 
                    coalesce(voting_rights, 0) as voting_rights_label
                from all_bills 
                    left join reproductive_rights using(bill_id, doc_id)
                        left join criminal_law using(bill_id, doc_id)
                            left join immigrant_rights using(bill_id, doc_id)
                                left join lgbt_rights using(bill_id, doc_id)
                                    left join racial_justice using(bill_id, doc_id)
                                        left join voting_rights using(bill_id, doc_id)
            """.format(tuple(bill_ids), as_of_date)

        try:
            cursor.execute(q)
            results = cursor.fetchall()
            self.sql_engine.commit()

            # Converting the results to a dict
            results_dict_list = [{
                'bill_id': x[0], 
                'doc_id': x[1], 
                'reproductive_rights_label': x[2], 
                'criminal_law_reform_label': x[3], 
                'immigrant_rights_label': x[4], 
                'lgbt_rights_label': x[5], 
                'racial_justice_label': x[6], 
                'voting_rights_label': x[7]}
                for x in results
            ]

        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)

        results_df = pd.DataFrame(results_dict_list)
        results_df['as_of_date'] = as_of_date
        
        return results_df

    def _retrieve_text_from_es(self, bill_doc_ids, query_size=100, text_key='description'):
        """
            Fetch the texts from Elastic search
            Args:
                bill_doc_ids: A list of dictionaries [{'bill_id':xx , 'doc_id':xx}]
                query_size: The size of to retrieve with elastic search
                text_key: The key in elastic search index that contains the text.
                            Could be 'description', 'title' or 'doc'

            return:
                A List of dictionaries. Each dictionary has the keys, bill_id, doc_id, text
        """

        # Creating the elasticsearch indexes for the bill_text index
        # document id takes the form <bill_id>_<doc_id>
        doc_indexes = ['{}_{}'.format(x['bill_id'], x['doc_id']) for x in bill_doc_ids]

        # creating the chunks of IDs to handle memory
        chunks = [doc_indexes[i:i + query_size] for i in range(0, len(doc_indexes), query_size)]

        texts = list()
        for i, chunk in enumerate(chunks):
            logging.info('Fetching indexes for chunk {}'.format(i))
            body = {'ids': chunk}
            res = self.es.mget(index=constants.BILL_TEXT_INDEX, body=body, _source_includes=[text_key, 'bill_id', 'doc_id'])
            docs = res['docs']

            texts_chunk = [
                {
                    'bill_id': x['_source']['bill_id'],
                    'doc_id': x['_source']['doc_id'],
                    'text': x['_source'][text_key]
                }
                for x in docs
            ]
            texts = texts + texts_chunk
            
        texts = pd.DataFrame(texts)

        return texts
