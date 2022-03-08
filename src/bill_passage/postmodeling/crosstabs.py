import os
import pandas as pd 
import numpy as np 
import logging
import json
import psycopg2
import itertools
from scipy import stats

from triage.component.postmodeling.crosstabs import CrosstabsConfigLoader, hr_lr_ttest
from triage.component.catwalk.storage import ProjectStorage
from psycopg2.extras import Json

from src.utils.general import read_yaml_file, copy_df_to_pg, get_db_conn


## The Functions that calculate the metrics for crosstabs
bin_mean = lambda df : df.mean(axis=0) 
bin_std = lambda df : df.std(axis=0)
bin_n = lambda df: pd.Series(index=df.columns, data=df.shape[0])

# given two likelihood bins, calculate the mean ratio
ratio_bins = lambda l_df, r_df : l_df.mean(axis=0) / r_df.mean(axis=0)


def bins_ttest(l_df, r_df):
    """Returns the t-test (T statistic and p value), comparing the features for the records in the two bins"""
    res = stats.ttest_ind(l_df.to_numpy(), r_df.to_numpy(), axis=0, nan_policy="omit", equal_var=False)

    r0 = pd.DataFrame(res[0], index=l_df.columns)
    r1 = pd.DataFrame(res[1], index=l_df.columns)
    r0['metric'] = 'ttest_T'
    r1['metric'] = 'ttest_p'
    
    return r0.append(r1)


single_bin_metrics = {
    "count": bin_n,
    "mean": bin_mean,
    "std": bin_std
}

multi_bin_metrics = {
    "mean_ratio": ratio_bins,
    'ttest':  bins_ttest
}

index_columns = ['entity_id', 'as_of_date']

def _fetch_model_matrix_info(db_engine, model_ids):
    """ For the given experiment and model groups, fetch the model_ids, and match them with their train/test matrix pairs 
        Args:
            model_groups (List[int]): A list of model groups in the experiment
            experiment_hashes (List[str]): Optional. A list of experiment hashes we are interested in.
                If this is provided, only the model_ids that are relevant to the given experiments will be returned

        Return: 
            A DataFrame that contains the relevant model_ids, train_end_times, and matrix information
    """

    q = """
        SELECT 
            model_id,
            train_end_time,
            model_hash,
            model_group_id,
            train_matrix_uuid,
            b.matrix_uuid as test_matrix_uuid
        FROM triage_metadata.models JOIN test_results.prediction_metadata b using(model_id)
        WHERE model_id in ({model_ids})"""

    args_dict = {
        'model_ids': ', '.join([str(x) for x in model_ids])
    }
    # Ensuring unique rows
    q = q + "\n GROUP BY 1, 2, 3, 4, 5, 6"
    
    q = q.format(**args_dict)

    return pd.read_sql(q, db_engine)


def crosstabs_model(engine, model_id, matrix_store, score_bands, single_bin_metrics=None, multi_bin_metrics=None):
    """ run crosstabs for one model
        Args:
            engine (psycopg2 engine): database engine
            model_id (int): The id of the trained model
            matrix_store (catwalk.storage.MatrixStore): The wrapper for the test matrix
            single_bin_metrics (Dict['metric_name': function]): Metrics to be calculated on a single bin
                A dictionary mapping the metric names and the function definitions to calc the metric
            multi_bin_metrics: Metrics to be calculated between multiple bins (currently only two).
                A dictionary metric names and the function definitions to calc the metric.
                At least one type of metrics need to be provided
    """

    if single_bin_metrics is None and multi_bin_metrics is None:
        raise ValueError('At least one type of metric need to be specified') 

    # fetch predictions
    q = """
        select
            entity_id, 
            as_of_date,
            score
        from test_results.predictions
        where model_id = {}
    """.format(model_id)

    predictions = pd.read_sql(q, engine)

    # Loading the matrix
    matrix = matrix_store.matrix_label_tuple[0]
    matrix = matrix.merge(predictions, on=['entity_id', 'as_of_date'])
    matrix.set_index(index_columns, inplace=True)

    # splitting the matrix into the score bands and assigning records to a scoreband dataframe
    # {'likelihood_bin_label': dataframe}
    dfs_for_bins = dict()

    # The dataframe that holds the results for both single bin and multi bin metrics we calculate
    results = pd.DataFrame()

    for band_label, limits in score_bands.items():
        # Limits of the score band / likelihood bins
        lower_lim = limits[0]
        upper_lim = limits[1]

        msk = (matrix['score'] >= lower_lim) & (matrix['score'] < upper_lim)
        df = matrix[msk]

        # We only need the feature data for the calculations
        dfs_for_bins[band_label] = df.drop('score', axis='columns')

        # we can calculate the single bin metrics here
        for metric_name, func in single_bin_metrics.items():
            logging.info('Calculating {} for likelihood bin -- {}'.format(metric_name, band_label))
            
            this_result = pd.DataFrame(func(df)).reset_index()
            this_result.fillna(0, inplace=True)
            this_result.columns = ['feature_name', 'value']
            
            this_result['metric'] = metric_name
            this_result['related_likelihood_bins'] = band_label

            results = results.append(this_result, ignore_index=True)


    # we calculate multi bin metrics for all possible ratios between bins 
    for bin_pair in itertools.permutations(score_bands.keys(), 2):

        # left / right
        left_df = dfs_for_bins[bin_pair[0]]
        right_df = dfs_for_bins[bin_pair[1]]
        
        for metric_name, func in multi_bin_metrics.items():
            logging.info('Calculating {} for likelihood bins -- {}'.format(metric_name, bin_pair))
            this_result = pd.DataFrame(func(left_df, right_df)).reset_index()
            this_result.fillna(0, inplace=True)

            # The ttest function returns the p-value and the t-stat, so we handle it differently
            # NOTE -- This is a crude way of preserving structure. Maybe there's a better way
            if metric_name == 'ttest':
                this_result.columns = ['feature_name', 'value', 'metric']
                this_result['related_likelihood_bins'] = ', '.join(bin_pair)
            else:
                this_result.columns = ['feature_name', 'value']
                this_result['metric'] = metric_name

                # The other metric is a mean ratio, so indicating the numerator and the denominator
                this_result['related_likelihood_bins'] = '/'.join(bin_pair)

            results = results.append(this_result, ignore_index=True)

    return results


def run_crosstabs(engine, crosstabs_config, single_bin_metrics=None, multi_bin_metrics=None):
    """run crosstabs for the given set of models
        Args:
            engine: 
            crosstabs_config:
            score_bands (dict): A dictionary that maps the bin label to the score threshlods of the bin.
                The key is the bin label, and the value is an array of two elements -- the lower limit and the upper limit.
                {label: [lower_limit, upper_limit]}
            single_bin_metrics (Dict['metric_name': function]): Metrics to be calculated on a single bin
                A dictionary mapping the metric names and the function definitions to calc the metric
            multi_bin_metrics: Metrics to be calculated between multiple bins (currently only two).
                A dictionary metric names and the function definitions to calc the metric.
                At least one type of metric need to be provided
    """

    if single_bin_metrics is None and multi_bin_metrics is None:
        raise ValueError('At least one type of metric need to be specified') 

    table_name = '{}.{}'.format(crosstabs_config.output['schema'], crosstabs_config.output['table'])
    logging.info('Checking whether the crosstabs table {} exist in DB. If not, creating'.format(table_name))
    _create_crosstabs_table_likelihood_bins(engine, schema=crosstabs_config.output['schema'], table=crosstabs_config.output['table'])

    model_info = _fetch_model_matrix_info(
        db_engine=engine,
        model_ids=crosstabs_config.model_ids
    )

    score_bands = crosstabs_config.thresholds['score_bins']
    matrix_storage_engine = ProjectStorage(crosstabs_config.project_path).matrix_storage_engine() 

    for row in model_info.itertuples(index=False):
        matrix_store = matrix_storage_engine.get_store(matrix_uuid=row.test_matrix_uuid)
        
        res = crosstabs_model(
            engine=engine,
            model_id=row.model_id,
            matrix_store=matrix_store,
            score_bands=score_bands,
            single_bin_metrics=single_bin_metrics,
            multi_bin_metrics=multi_bin_metrics
        )        

        res['model_id'] = row.model_id
        res['train_end_time'] = row.train_end_time

        logging.info('Writing crosstab results to DB')
        try:
            copy_df_to_pg(
                engine=engine,
                table_name=table_name,
                df=res,
            )
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            raise psycopg2.DatabaseError(error)             

    logging.info('Crosstabs calculation sucessfully completed!')

       







# def run_crosstabs2(engine, crosstabs_config, single_bin_metrics, multi_bin_metrics=None): 
    
#     table_name = '{}.{}'.format(crosstabs_config.output['schema'], crosstabs_config.output['table'])
#     logging.info('Checking whether the crosstabs table {} exist in DB. If not, creating'.format(table_name))
#     _create_crosstabs_table_likelihood_bins(engine, schema=crosstabs_config.output['schema'], table=crosstabs_config.output['table'])

#     logging.info('Fetchin the prediction data for the relevant model_ids and as_of_dates')
#     crosstabs_query = """
#         with models_list_query as (
#             {models_list_query}
#         ), as_of_dates_query as (
#             {as_of_dates_query}
#         ),models_dates_join_query as (
#             {models_dates_join_query}
#         ),features_query as (
#             {features_query}
#         ), predictions_query as (
#             {predictions_query}
#         )
#         select * from predictions_query
#             left join features_query f using (model_id,entity_id, as_of_date)
#         """.format(
#         models_list_query=crosstabs_config.models_list_query,
#         as_of_dates_query=crosstabs_config.as_of_dates_query,
#         models_dates_join_query=crosstabs_config.models_dates_join_query,
#         features_query=crosstabs_config.features_query,
#         predictions_query=crosstabs_config.predictions_query,
#     )

#     if len(crosstabs_config.entity_id_list) > 0:
#         crosstabs_query += " where entity_id=ANY('{%s}') " % ", ".join(map(str, crosstabs_config.entity_id_list))
#     crosstabs_query += "  order by model_id, as_of_date, rank_abs asc;"

#     df = pd.read_sql(crosstabs_query, engine)

#     if len(df) == 0:
#         raise ValueError("No data could be fetched.")

#     logging.info('Fetched {} predictions'.format(len(df)))

#     # Separating feature and non_feature columns
#     non_feature_columns = ["model_id", "as_of_date", "entity_id", "score", "rank_abs", "rank_pct", "label_value"]
#     feature_columns = [x for x in df if x not in non_feature_columns]

#     # For each model_id, as_of_date pair, calculate the cross tabs
#     groupby_obj = df.groupby(["model_id", "as_of_date"])  

#     for group, _ in groupby_obj:
#         df_grp = groupby_obj.get_group(group)

#         logging.info('Generating crosstabs for model_id: {}, as_of_date: {}'.format(group[0], group[1]))

#         res = calc_crosstabs_likelihood_bins(
#             df=df_grp,
#             likelihood_bins=crosstabs_config.thresholds['score_bins'],
#             feature_names=feature_columns,
#             crosstab_functions_single_bin=single_bin_metrics,
#             crosstab_functions_multi_bin=multi_bin_metrics
#         )

#         res['model_id'] = group[0]
#         res['as_of_date'] = group[1]

#         logging.info('Writing crosstab results to DB')
#         try:
#             copy_df_to_pg(
#                 engine=engine,
#                 table_name=table_name,
#                 df=res,
#             )
#         except (Exception, psycopg2.DatabaseError) as error:
#             logging.error(error)
#             raise psycopg2.DatabaseError(error)             

#     logging.info('Crosstabs calculation sucessfully completed!')

        
# def calc_crosstabs_likelihood_bins(df, likelihood_bins, feature_names, crosstab_functions_single_bin, crosstab_functions_multi_bin=None):
#     """Calculates crosstabs across the passage likelihood bins for a given model_id, and as_of_date pair.
    
#         Args: 
#             df: A dataframe that contains predictions & feature values for one model_id, as_of_date pair, assumes that only the relevant columns are passed
            
#             likelihood_bins (dict): A dictionary that maps the bin label to the score threshlods of the bin.
#                                     The key is the bin label, and the value is an array of two elements -- the lower limit and the upper limit.
#                                     {label: [lower_limit, upper_limit]}
#             feature_names (list): The list of relevant feature names (in triage format)

#             crosstab_functions_single_bin (dict): A dictionary that maps metric names to functions that would return the calculated metric. 
#                                                 It is assumed that each metric has its own function. These metrics will be calculated 
#                                                 for all the likelihood bins. Each function only accepts a single dataframe (only one bin)

#             crosstab_functions_multi_bin (dict) optional: A dictionary that maps metrics to functins that compare across bins.
#                                                     Currently only handing pairs of bins

#         Return:
#             A DataFrame that contains the these columns -- feature_name, metrc, value, likelihood_bin_info (jsonb)
#     """

#     results = pd.DataFrame()
#     for bin_label, score_limits in likelihood_bins.items():

#         # Filtering the predictions in the bin
#         low_lim_msk = (df['score'] >= score_limits[0])
#         upper_lim_msk = (df['score'] < score_limits[1])

#         # Making sure the score == 1.0 is included
#         if score_limits[1] == 1:
#             upper_lim_msk = (df['score'] <= score_limits[1])

#         bin_df = df.loc[low_lim_msk & upper_lim_msk, feature_names]

#         # calculating single bin metrics for
#         for metric_name, func in crosstab_functions_single_bin.items():
#             logging.info('Calculating {} for likelihood bin -- {}'.format(metric_name, bin_label))
            
#             this_result = pd.DataFrame(func(bin_df)).reset_index()
#             this_result.fillna(0, inplace=True)
#             this_result.columns = ['feature_name', 'value']
            
#             this_result['metric'] = metric_name
#             this_result['related_likelihood_bins'] = bin_label

#             results = results.append(this_result, ignore_index=True)

#     if crosstab_functions_multi_bin is None:
#         return results

#     logging.info('Multibin functions are defined')
#     logging.info('Formatting the likelihood bin pairs for calculating pairwise comparison metrics')
    
#     # We need to ensure that the bins are ordered w.r.t their relevant probablity to ensure consistent ratio calculations
#     all_bin_pairs = list()
#     for bin_pair in itertools.combinations(likelihood_bins.keys(), 2):
        
#         # reverse the order if the bins are not ordered higher-first using the lower limit of the thresholds
#         if likelihood_bins[bin_pair[0]][0] < likelihood_bins[bin_pair[1]][0]:
#             t = (bin_pair[1], bin_pair[0])
#             all_bin_pairs.append(t)
#         else:
#             all_bin_pairs.append(bin_pair)

    
#     for bin_pair in all_bin_pairs:
#         # high bin
#         score_lims = likelihood_bins[bin_pair[0]]
#         low_lim_msk = (df['score'] >= score_lims[0])
#         upper_lim_msk = (df['score'] < score_lims[1]) 
#         if score_limits[1] == 1:
#             upper_lim_msk = (df['score'] <= score_limits[1])

#         high_df = df.loc[low_lim_msk & upper_lim_msk, feature_names]

#         # low bin
#         score_lims = likelihood_bins[bin_pair[1]]
#         low_lim_msk = (df['score'] >= score_lims[0])
#         upper_lim_msk = (df['score'] < score_lims[1])

#         low_df = df.loc[low_lim_msk & upper_lim_msk, feature_names]

#         for metric_name, func in crosstab_functions_multi_bin.items():
#             logging.info('Calculating {} for likelihood bins -- {}'.format(metric_name, bin_pair))

#             this_result = pd.DataFrame(func(high_df, low_df)).reset_index()
#             this_result.fillna(0, inplace=True)
            
#             # The ttest function returns the p-value and the t-stat, so we handle it differently
#             # NOTE -- This is a crude way of preserving structure. Maybe there's a better way
#             if metric_name == 'ttest':
#                 this_result.columns = ['feature_name', 'value', 'metric']
#             else:
#                 this_result.columns = ['feature_name', 'value']
#                 this_result['metric'] = metric_name
            
#             this_result['related_likelihood_bins'] = ', '.join(bin_pair)

#             results = results.append(this_result, ignore_index=True)
    
#     return results


def _create_crosstabs_table_likelihood_bins(engine, schema, table):
    """Creates the table we need to store the crosstabs results. If the table already exists, this is ignored"""

    q = """
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            model_id int,
            train_end_time timestamp,
            metric varchar,
            related_likelihood_bins varchar,
            feature_name varchar,
            value float
        )
    """.format(
        schema=schema,
        table=table
    )

    cursor = engine.cursor()
    try: 
        cursor.execute(q)
        engine.commit()  
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        raise psycopg2.DatabaseError(error)


if __name__=='__main__':
    creds_folder = '../../../conf/local/'
    creds_path = os.path.join(creds_folder, 'credentials.yaml')

    db_conn = get_db_conn(creds_path)

    config_file = 'crosstabs_config.yaml'
    crosstabs_config = CrosstabsConfigLoader(config_path=config_file)

    run_crosstabs(
        engine=db_conn,
        crosstabs_config=crosstabs_config,
        single_bin_metrics=single_bin_metrics,
        multi_bin_metrics=multi_bin_metrics
    )
