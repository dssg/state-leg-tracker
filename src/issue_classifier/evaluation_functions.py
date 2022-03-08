import pandas as pd
import logging
import psycopg2

from evaluation_metrics import get_roc_auc, get_precision_at_k, get_recall_at_k


def get_model_predictions(model, matrix):
    """ Get predictions from a trained model for a matrix and a issue area 
        args:
            model: The trained model
            matrix: Matrix to be scored. Should be indexed by the (entity_id, as_of_date) and should only contain the features
            type: whether train or test
    """

    # The model object should have a predict_proba function. 
    # TODO: Kind of confined to sklearn architecture. Generalize.

    scores = model.predict_proba(matrix.values)

    # The score we are intersted in is the score for the "1" class
    predictions = pd.DataFrame(scores[:, 1], index=matrix.index, columns=['score'])

    return predictions


# TODO: Testing
def evaluate_model(predictions, label_values, metric_groups):
    """ calculate the evaluations for a given set of model predictions, labels and a set of metric groups"""

    function_mapping = {
        'precision@': get_precision_at_k,
        'recall@': get_recall_at_k,
        'roc_auc': get_roc_auc
    }

    results = dict()

    for met_group in metric_groups:
        for met in met_group['metrics']:
            if met is 'roc_auc':
                results[met] = function_mapping[met](predictions, label_values)
            else:
                for thresh_type, values in met_group['thresholds'].items():
                    if thresh_type == 'percentiles':                   
                        metric_vals = [{ str(k)+'pct':  function_mapping[met](predictions, label_values, k*0.01)} for k in values]
                        results[met] = metric_vals    
                    else:
                        metric_vals = [{ str(k):  function_mapping[met](predictions, label_values, k)} for k in values]
                        results[met] = metric_vals

    return results


def write_to_predictions(
    engine,
    predictions, 
    model_id,
    matrix_uuid, 
    experiment_hash,  
    label_values, 
    issue_area,
    schema,
    table):
    """
        Write the predictions to the respective predictions table
        args:
            engine: sql engine
            predictions: Dataframe indexed by entity_id, as_of_date with one column named 'score'
            model_id: ID of the model
            matrix_uuid: the uuid of the matrix used
            experiment_hash: Hash of the modeling experiment
            matrix_type: Train or test matrix
            label_values: A Dataframe indexed by entitty_id, as_of_date with one column named 'label_value'
            issue_area: The ACLU key issue area the model is classifying
    """
    # print(label_values.head())
    # predictions = predictions.join(label_values, how='inner')
    predictions['model_id'] = model_id
    predictions['matrix_uuid'] = matrix_uuid
    predictions['experiment_hash'] = experiment_hash
    predictions['issue_area'] = issue_area
    predictions['label_value'] = label_values

    # Resetting the entity_id, as_of_date indexes
    predictions = predictions.reset_index()

    # TODO: Improve with OHIO

    preds_tuple = [tuple(x) for x in predictions.to_numpy()]
    # predictions.to_sql(table, engine, schema=schema, if_exists='append',index=False)
    cols = ', '.join(list(predictions.columns))
    cursor = engine.cursor()

    q = "insert into {}.{} ({}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)".format(
        schema, table, cols
    )
    
    try:
        cursor.executemany(q, preds_tuple)
        # engine.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        raise psycopg2.DatabaseError(error)

    # # engine.commit()


# TODO
def write_to_evaluations(model_id):
    pass
