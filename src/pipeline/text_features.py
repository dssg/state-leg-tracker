import psycopg2
import logging
import uuid
import csv
import boto3

import src.utils.project_constants as constants

from psycopg2.extras import execute_batch, Json

from src.pipeline.generate_timesplits import get_time_splits
from src.utils.general import get_db_conn, get_elasticsearch_conn, get_issue_area_configuration, get_s3_credentials
from src.pipeline.tf_id_features import tf_idf_features


def _get_ids(cohort):
    """
    Get the ids of the documents that exist between a selected period of time
    :param from_date: Start date
    :param to_date: End date
    :return:
    """
    bill_ids = [element[0] for element in cohort]

    db_conn = get_db_conn("../../conf/local/credentials.yaml")
    cursor = db_conn.cursor()

    q = """ 
       with all_bills as( 
        select bill_id, max(doc_id) as doc_id
        from clean.bill_docs
        where bill_id in {}
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
   
   select bill_id as entity_id, doc_id, coalesce(reproductive_rights, 0), 
   coalesce(criminal_law_reform, 0), coalesce(immigrant_rights, 0), coalesce(lgbt_rights, 0), 
   coalesce(racial_justice, 0), coalesce(voting_rights, 0)
   from all_bills 
   left join reproductive_rights using(bill_id, doc_id)
   left join criminal_law using(bill_id, doc_id)
   left join immigrant_rights using(bill_id, doc_id)
   left join lgbt_rights using(bill_id, doc_id)
   left join racial_justice using(bill_id, doc_id)
   left join voting_rights using(bill_id, doc_id)
    """.format(tuple(bill_ids))

    try:
        cursor.execute(q)
        ids = cursor.fetchall()
        db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)

    return ids


def _retrieve_text_from_es(ids):
    """
    Retrieve content from specific documents
    :param ids: List of doc ids to retrieve text from
    :return: List of tuples with id and text
    """
    #index_ids = ['_'.join([str(element[0]), str(element[1])]) for element in ids]
    index_ids = [element[0] for element in ids]

    es = get_elasticsearch_conn('../../conf/local/credentials.yaml')
    texts = []

    chunks = [index_ids[i:i + 100] for i in range(0, len(index_ids), 100)]
    print(len(chunks))
    for i, chunk in enumerate(chunks):
        print(i)
        body = {'ids': chunk}
        res = es.mget(index=constants.BILL_META_INDEX, body=body, _source_includes=['description'])
        docs = res['docs']
        texts_chunk = [element['_source']['description'] for element in docs]
        texts.append(texts_chunk)

    return texts


def _save_ids(ids, as_of_date):
    """
    Create table <uuid_matrix_entity_id> on the features schema
    :param ids: List of ids in the time chop
    :return: Uuid of the time chop
    """
    matrix_uuid = uuid.uuid4().hex

    db_conn = get_db_conn("../../conf/local/credentials.yaml")
    cursor = db_conn.cursor()

    # change role
    q = "set role rg_staff"
    try:
        cursor.execute(q)
        db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)

    values = [(element[0], as_of_date) for element in ids]
    # entity_id, as_of_date
    table_name = matrix_uuid + "_matrix_entity_date"
    q = 'create table issue_classifier_features."{}"(entity_id integer, as_of_date timestamp without time zone)'.format(table_name)

    try:
        cursor.execute(q)
        db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)

    q = 'insert into issue_classifier_features."{}" (entity_id, as_of_date) VALUES(%s, %s)'.format(table_name)

    try:
        execute_batch(cursor, q, values)
        db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)

    return matrix_uuid


def _save_matrix(type, time_chop, matrix_uuid, time_split_index):
    """
    Stores the matrix on the metadata matrices table
    :param type: Type of matrix either train or test
    :param time_chop: Particular time split
    :param matrix_uuid: UUID of the matrix
    :param time_split_index: Number of split
    :return:
    """
    issue_area_conf = get_issue_area_configuration("../issue_classifier/experiment_config/issue_area_config.yaml")
    temporal_config = issue_area_conf['temporal_config']

    matrix_type = type
    feature_start_time = time_chop['feature_start_time']
    indices = ["entity_id", "as_of_date"]
    # TODO pass the experiment id
    built_by_experiment = ""

    db_conn = get_db_conn("../../conf/local/credentials.yaml")
    cursor = db_conn.cursor()

    if type == 'train':
        matrix_id = "_".join([str(time_chop['train_matrix']['as_of_times'][0]),
                              str(time_chop['train_matrix']['as_of_times'][-1])])

        # most be calculated: last as_of_date - first as_of_date of this time split
        lookback_duration = time_chop['train_matrix']['as_of_times'][0] -\
                            time_chop['train_matrix']['as_of_times'][-1]
        as_of_times = [str(element) for element in time_chop['train_matrix']['as_of_times']]

        matrix_metadata = {'as_of_date_frequency': temporal_config['training_as_of_date_frequencies'],
                           'as_of_times': as_of_times,
                           'end_time': str(time_chop['train_matrix']['matrix_info_end_time']),
                           'feature_start_time': str(feature_start_time),
                           'first_as_of_time': str(time_chop['train_matrix']['first_as_of_time']),
                           'indices': indices,
                           'last_as_of_time': str(time_chop['train_matrix']['last_as_of_time']),
                           'matrix_id': matrix_id,
                           'matrix_info_end_time': str(time_chop['train_matrix']['matrix_info_end_time']),
                           'matrix_type': matrix_type,
                           'max_training_history': temporal_config['max_training_histories'],
                           'training_as_of_date_frequency': temporal_config["training_as_of_date_frequencies"],
                           'training_label_timespan': temporal_config["label_timespans"]}

        q = "insert into issue_classifier_metadata.matrices VALUES({})".format(", ".join(['%s'] * 8))
        var = (
            matrix_id,
            matrix_uuid,
            matrix_type,
            feature_start_time,
            lookback_duration,
            Json(matrix_metadata),
            built_by_experiment,
            time_split_index
        )

    if type == 'test':
        matrix_id = "_".join([str(time_chop['test_matrices'][0]['as_of_times'][0]),
                              str(time_chop['test_matrices'][0]['matrix_info_end_time'])])

        # most be calculated: last as_of_date - first as_of_date of this time split
        lookback_duration = time_chop['test_matrices'][0]['last_as_of_time'] - \
                            time_chop['test_matrices'][0]['first_as_of_time']
        as_of_times = [str(element) for element in time_chop['test_matrices'][0]['as_of_times']]

        matrix_metadata = {'as_of_date_frequency': temporal_config['training_as_of_date_frequencies'],
                           'as_of_times': as_of_times,
                           'end_time': str(time_chop['train_matrix']['matrix_info_end_time']),
                           'feature_start_time': str(feature_start_time),
                           'first_as_of_time': str(time_chop['train_matrix']['first_as_of_time']),
                           'indices': indices,
                           'last_as_of_time': str(time_chop['train_matrix']['last_as_of_time']),
                           'matrix_id': matrix_id,
                           'matrix_info_end_time': str(time_chop['train_matrix']['matrix_info_end_time']),
                           'matrix_type': matrix_type,
                           'max_training_history': temporal_config['max_training_histories'],
                           'training_as_of_date_frequency': temporal_config["training_as_of_date_frequencies"],
                           'training_label_timespan': temporal_config["label_timespans"]}

        q = "insert into issue_classifier_metadata.matrices VALUES({})".format(", ".join(['%s'] * 8))
        var = (
            matrix_id,
            matrix_uuid,
            matrix_type,
            feature_start_time,
            lookback_duration,
            Json(matrix_metadata),
            built_by_experiment,
            time_split_index
        )

    try:
        cursor.execute(q, var)
        db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def _process_csr_matrix(matrix, vocabulary, row):
    """
    Process a compressed row sparse matrix to retrieve the word vector representation retrieving the
    corresponding word associated to each vectorized value.

    For more information of how to deal with a CSR matrix:
    https://en.wikipedia.org/wiki/Sparse_matrix and
    http://www.netlib.org/utk/people/JackDongarra/etemplates/node373.html

    :param matrix: word vectorized matrix
    :param vocabulary: words on the corpus
    :return:
    """

    #print("row:", row)
    row_start = matrix.indptr[row]
    row_end = matrix.indptr[row + 1]

    # retrieve the nnz values of the nth row
    tf_idf_values = matrix.data[row_start:row_end]

    # retrieve correspondent words for the columns with nnz values
    words_indices = matrix.indices[row_start:row_end]
    words = [vocabulary[element] for element in words_indices]
    #print("got words")

    features = {}
    # by default all words in the vocabulary have 0.0
    for word in vocabulary:
        features[word] = 0
    # update those words with an actual value different to 0.0
    for i, element in enumerate(tf_idf_values):
        features[words[i]] = element

    return features


def _save_features(matrix_uuid, matrix, vocabulary, ids, as_of_date):
    """
    Stores the labels and features for each time split
    :param matrix_uuid: uuid for this matrix
    :param matrix: Matrix with text vectorized
    :param vocabulary: Words from the term document matrix
    :param ids: List of tuples with bill_id, doc_id, and labels from issue_areas
    :param as_of_date: As of date
    :return:
    """
    # process csr matrix
    with open("../../matrices/{}.csv".format(matrix_uuid), "w", newline='') as csvfile:
        for row in range(matrix.shape[0]):
            print("row: ", row)
            features_dict = _process_csr_matrix(matrix, vocabulary, row)
            print("features dictionary")

            element = ids[row]
            # warning: we don't include element[1] because it corresponds to the doc_id, we don't require to store it
            # on this table.
            features_dict['as_of_date'] = as_of_date
            features_dict['entity_id'] = element[0]
            features_dict['reproductive_rights_label'] = element[2]
            features_dict['criminal_law_reform_label'] = element[3]
            features_dict['immigrant_rights_label'] = element[4]
            features_dict['lgbt_rights_label'] = element[5]
            features_dict['racial_justice_label'] = element[6]
            features_dict['voting_rights_label'] = element[7]

            if row == 0:
                keys = features_dict.keys()
                dict_writer = csv.DictWriter(csvfile, keys)
                dict_writer.writeheader()
                dict_writer.writerow(features_dict)
            else:
                dict_writer.writerow(features_dict)


def _get_cohort(as_of_date):
    """
    Get cohort query from triage experiment_config
    :param as_of_date:
    :return:
    """
    q_cohort = get_issue_area_configuration('../issue_classifier/experiment_config/issue_area_config.yaml')\
        ['cohort_config']['query'].format(as_of_date=as_of_date)

    db_conn = get_db_conn('../../conf/local/credentials.yaml')
    cursor = db_conn.cursor()

    try:
        cursor.execute(q_cohort)
        cohort = cursor.fetchall()
        db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)

    return cohort


def _get_timechops():
    """Get timechops slices"""

    # get issue area classifier configuration
    issue_classifier_conf = get_issue_area_configuration("../issue_classifier/experiment_config/issue_area_config.yaml")

    temporal_config = issue_classifier_conf['temporal_config']
    time_chops = get_time_splits(temporal_config)

    return time_chops


def _process_time_chop(type, as_of_date, algorithm, time_chop, time_split_index):
    """
    Processes each time split by getting the cohort for this split, the bill ids and the labels for the cohort.

    Stores the bill_id as the `entity_id` and the `as of_date` on their particular <uuid>_matrix_entity_date table.

    Retrieves and vectorizes the text of the bills that take part in this cohort.

    Stores the vectorization of the text matrix on the matrices table.

    Stores the features obtained for this time split on the features table including their labels and the vocabulary.

    :param type: Type of matrix, either train or test
    :param as_of_date: As of date
    :param algorithm: Algorithm to use for vectorization of the text
    :param time_chop: Particular time split
    :return:
    """
    cohort = _get_cohort(as_of_date)
    ids = _get_ids(cohort)
    matrix_uuid = _save_ids(ids, as_of_date)
    texts = _retrieve_text_from_es(ids)
    if algorithm == "tf-idf":
        #text_list = [element[1] for element in texts]
        matrix, vocabulary = tf_idf_features(texts)
    else:
        pass
    print("saving matrix")
    _save_matrix(type, time_chop, matrix_uuid, time_split_index)
    print("saving features")
    _save_features(matrix_uuid, matrix, vocabulary, ids, as_of_date)


def get_text_features():
    """
    Generates text features with the text of bills for the different time chops and store them in s3 and postgres
    :return:
    """

    # get time splits
    time_splits = _get_timechops()
    algorithm = get_issue_area_configuration("../issue_classifier/experiment_config/issue_area_config.yaml")\
        ['features']['type']

    for i, time_chop in enumerate(time_splits):
        print("time split: ", i)
        train_splits = time_chop['train_matrix']['as_of_times']
        print("+ train splits: ", len(train_splits))
        test_splits = time_chop['test_matrices'][0]['as_of_times']
        print("+ test splits: ", len(test_splits))

        # train matrices processing
        for j, as_of_date in enumerate(train_splits):
            print("  train: ", j)
            _process_time_chop("train", as_of_date, algorithm, time_chop, i)

        # test matrices processing
        for k, as_of_date in enumerate(test_splits):
            print("  test: ", k)
            _process_time_chop("test", as_of_date, algorithm, time_chop, i)


