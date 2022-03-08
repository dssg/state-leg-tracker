import pandas as pd 
from sklearn.metrics import roc_auc_score


def _get_topk(predictions, labels, k):
    """ get the top-k records 
        args:
            preditions: dataframe indexed by entity_id, as_of_date which has the 'score'
            labels: dataframe indexed by entity_id, as_of_date that has the label_value
            k: top percentile or top number of records to be considered.
                Could be float or int. If float, percentile, if int num records. 

        return:
            Dataframe indexed by (entity_id, as_of_date) with columns, [score, label_value]
    """ 

    if k <= 1:
        num_recs = predictions.shape[0]
        k_recs = int(num_recs * k)
    else:
        k_recs = k

    # combining the predictions and the labels
    joined = predictions.join(labels, how='inner')
    top_k = joined.sort_values('score', ascending=False).iloc[:k_recs]

    return top_k


def get_precision_at_k(predictions, labels, k):
    """ Calculate the precision at top-k 
        args:
            preditions: dataframe indexed by entity_id, as_of_date which has the 'score'
            labels: dataframe indexed by entity_id, as_of_date that has the label_value
            k: top percentile or top number of records to be considered.
                Could be float or int. If float, percentile, if int num records. 
    """   
    top_k = _get_topk(predictions, labels, k)
    
    label_counts = top_k.groupby('label_value').count()['score']
    
    precision = label_counts.loc[1]/top_k.shape[0]

    return precision

def get_recall_at_k(predictions, labels, k):
    """
        Calculate recall at k
        args:
            preditions: dataframe indexed by entity_id, as_of_date which has the 'score'
            labels: dataframe indexed by entity_id, as_of_date that has the label_value
            k: top percentile or top number of records to be considered.
                Could be float or int. If float, percentile, if int num records. 
    """

    top_k = _get_topk(predictions, labels, k)
    label_counts = top_k.groupby('label_value').count()['score']

    label_counts_universe = labels.groupby('label_value').count()

    recall = label_counts.loc[1]/ label_counts_universe.loc[1]

    return recall

def get_roc_auc(predictions, labels):
    """
        Calculate Area under the ROC
        args:
            preditions: dataframe indexed by entity_id, as_of_date which has the 'score'
            labels: dataframe indexed by entity_id, as_of_date that has the label_value
            k: top percentile or top number of records to be considered.
                Could be float or int. If float, percentile, if int num records. 
    """

    y_hat = predictions['score'].values
    y = labels['label_values'].values

    auc = roc_auc_score(y_true=y, y_score=y_hat)

    return auc