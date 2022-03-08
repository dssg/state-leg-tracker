import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import json
import itertools

def get_triage_components_experiment(sql_engine, experiment_hash, model_group):
    """ get the model and matrix information for a experiment hash and a model group
        Args:
            sql_engine: The sql connection engine
            experiment_hash: The triage experiment hash
            model_group: The model group that you want to analyse. Assumes that audition was used to select the model groups that you are interested in 

    """

    q = """
        with models as (
            select 
                model_id, 
                model_hash, 
                model_type
            from triage_metadata.experiment_models join triage_metadata.models using(model_hash)
            where experiment_hash='{}' and model_group_id={}
        ),
        train_mats as (
            select 
                model_id,
                model_hash,
                model_type,
                matrix_uuid as train_mat
            from train_results.prediction_metadata join models using (model_id)
        ),
        test_mats as (
            select 
                model_id,
                matrix_uuid as test_mat
            from test_results.prediction_metadata join models using (model_id)
        )
        select 
            model_id,
            model_hash,
            -- model_type,
            train_mat,
            test_mat
        from train_mats join test_mats using (model_id)
    """.format(experiment_hash, model_group)

    components = pd.read_sql(q, sql_engine)

    return components


def get_triage_model_predictions(sql_engine, model_id):
    """Fetch the model predictions into a pandas Dataframe

        Args:
            sql_engine: sql engine
            model_id: The triage model id to get the predictions for
    """

    q = """
        select 
            entity_id, as_of_date, score, label_value 
        from test_results.predictions where model_id={};
    """.format(model_id)

    model_predictions = pd.read_sql(q, sql_engine)

    return model_predictions


def get_top_k(model_predictions, k=0.1):
    """ Fetch the top-k of sorted by the model score
        Args:
            model_predictions: 
            k:  the percentage or number of entities to be included in top_k, If (0,1], percentage is considered. 
                If integer, the number of records are returned
    
    """
    predictions = model_predictions.sort_values('score', ascending=False)

    if k <= 1:
        k_recs = int(predictions.shape[0] * k)
    else:
        k_recs = int(k)

    top_k = predictions.iloc[:k_recs]

    return top_k 


def get_precision_at_k(top_k):
    """ Calculate precision at the top_k"""
    msk = top_k['label_value'] == 1

    num_correct = top_k[msk].shape[0]
     
    precision = round(num_correct/top_k.shape[0], 4)

    return precision


def get_recall_at_k(top_k, model_predictions):
    """calculate recall at top_k"""
    msk = top_k['label_value'] == 1
    num_correct = top_k[msk].shape[0]

    label_counts = model_predictions.groupby('label_value').count()['entity_id']
    recall = num_correct / label_counts.loc[1]

    return recall


def get_pr_k_curve_model(sql_engine, model_id, plot=True, save_target=None): 
    """
        get precision and recall curve for a model
    """

    predictions = get_triage_model_predictions(sql_engine, model_id)

    k_values = list(np.arange(0.1, 1.1, 0.1))
    pr_k = pd.DataFrame()

    for k in k_values:
        d = dict()
        d['k'] = k

        top_k = get_top_k(predictions, k)
        d['precision'] = get_precision_at_k(top_k)
        d['recall'] = get_recall_at_k(top_k, predictions)

        pr_k = pr_k.append(d, ignore_index=True)

    if plot:
        fig, ax1 = plt.subplots()
        ax1.plot(pr_k['k'], pr_k['precision'], label='precision')
        ax1.plot(pr_k['k'], pr_k['recall'], label='recall')
        plt.legend()

        if save_target is not None:
            plt.savefig(save_target, dpi=300)

    return pr_k


# TODO: Improve performance
def parse_sparse_bow_json(json_file):
    """loading the sparse BoW feature matrix stored as a JSON into a dense dataframe
        The JSON should contain a dictionary of the form: 
            'matrix': {'row_idx, col_idx': value}
            'vocabulary': {'word': col_idx}
            'id_mapping': {'enity_id, as_of_date': row_idx}
    """
    d = json.loads(json_file)

    # Converting the string dictionary keys in to a tuple of (int, str)
    d['id_mapping'] = {
        (int(k.split(', ')[0]), k.split(', ')[1]): v for k, v in d['id_mapping'].items()
    }

    d['matrix'] = {
        (int(k.split(',')[0]), int(k.split(',')[1])): v for k, v in d['matrix'].items()
    }

    # Zeroes dataframe
    df = pd.DataFrame(0, index=d['id_mapping'].keys(), columns=d['vocabulary'].keys())

    # Cartesian product of row, col indexes
    prod = itertools.product(d['id_mapping'].keys(), d['vocabulary'].keys())

    # setting the non zero elements
    for idx, word in prod:
        row_idx = d['id_mapping'][idx]
        col_idx = d['vocabulary'][word]
        
        if (row_idx, col_idx) in d['matrix']:
            df.loc[idx, word] = d['matrix'][(row_idx, col_idx)]

    # setting the index names
    df.index.names=['entity_id', 'as_of_date']
    
    # resetting the index to be consistent with other load functions
    df = df.reset_index()

    return df