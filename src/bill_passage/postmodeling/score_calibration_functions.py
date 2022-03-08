import os
import sys
import yaml
import logging

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


def get_passage_prob_model_score_buckets(engine, model_id, num_buckets=10):
    """Arrange model scores into buckets"""

    q = """
        with predictions as (
            select 
                a.*, b.train_end_time
            from test_results.predictions a join triage_metadata.models b using(model_id)
            where model_id = {model_id}
            order by score desc
        )
        select 
            max(model_id) as model_id,
            max(train_end_time) as train_end_time,
            width_bucket(score, 0, 1, {num_buckets}) as score_buckets,
            (sum(label_value)::float/count(*)) * 100 as passage_probability,
            round(min(score), 2) as bucket_lower_bound,
            count(*) as num_records_in_bucket,
            sum(label_value) as num_ones_in_bucket
        from predictions
        group by score_buckets
        order by score_buckets
    """.format(model_id=model_id, num_buckets=num_buckets)

    df = pd.read_sql(q, engine)

    tot_recs = df['num_records_in_bucket'].sum()
    tot_ones = df['num_ones_in_bucket'].sum()
    df['tot_ones'] = tot_ones
    df['tot_instances'] = tot_recs
    df['pct_records_in_bucket'] = df['num_records_in_bucket']/tot_recs
    df['pct_ones_in_bucket'] = df['num_ones_in_bucket']/tot_ones
    df['prevelance'] = (float(tot_ones) / tot_recs) * 100

    return df


def get_passage_prob_model_score_deciles(engine, model_id):
    score_perc = """
        with predictions as (
            select 
                a.*, b.train_end_time
            from test_results.predictions a join triage_metadata.models b using(model_id)
            where model_id={model_id}
            order by score desc
        ),
        buckets as (
            select 
                predictions.*,
                case 
                    when score > d90 then 90
                    when (score > d80) and (score <= d90) then 80
                    when (score > d70) and (score <= d80) then 70
                    when (score > d60) and (score <= d70) then 60
                    when (score > d50) and (score <= d60) then 50
                    when (score > d40) and (score <= d50) then 40
                    when (score > d30) and (score <= d40) then 30
                    when (score > d20) and (score <= d30) then 20
                    when (score > d10) and (score <= d20) then 10
                    else 0
                end as decile_lower_bound
            from (
                select 
                    percentile_cont(0.90) within group (order by score) as d90,
                    percentile_cont(0.80) within group (order by score) as d80,
                    percentile_cont(0.70) within group (order by score) as d70,
                    percentile_cont(0.60) within group (order by score) as d60,
                    percentile_cont(0.50) within group (order by score) as d50,
                    percentile_cont(0.40) within group (order by score) as d40,
                    percentile_cont(0.30) within group (order by score) as d30,
                    percentile_cont(0.20) within group (order by score) as d20,
                    percentile_cont(0.10) within group (order by score) as d10
                from predictions
            ) as t, predictions
        )
        select 
            max(model_id) as model_id,
            max(train_end_time) as train_end_time,
            decile_lower_bound,
            count(*) as num_records_in_bucket,
            sum(label_value) as num_ones_in_bucket,
            (sum(label_value)::float/count(*)) * 100 as passage_probability
        from buckets
        group by decile_lower_bound
        order by decile_lower_bound
    """.format(model_id)

    temp = pd.read_sql(score_perc, engine)
    tot_recs = temp['num_records_in_bucket'].sum()
    tot_ones = temp['num_ones_in_bucket'].sum()

    temp['frac_records_in_bucket'] = temp['num_records_in_bucket']/tot_recs
    temp['frac_ones_in_bucket'] = temp['num_ones_in_bucket']/tot_ones
    temp['base_prevelance'] = (float(tot_ones) /tot_recs) * 100
    temp['tot_ones'] = tot_ones
    temp['tot_instances'] = tot_recs

    return temp


def get_score_distribution(engine, model_id):
    q = """select score from test_results.predictions where model_id={}""".format(model_id)
    model_scores = pd.read_sql(q, engine)
    sns.distplot(model_scores['score'], kde=False)
    plt.show()


def passage_probability_over_threshold(engine, model_id, threshold):
    q = """
        select 
            score,
            label_value 
        from test_results.predictions where model_id={} and score >={}
    """.format(model_id, threshold)

    df = pd.read_sql(q, engine)

    num_ones = df['label_value'].sum()

    passage_prob = float(num_ones) / len(df)

    return passage_prob

 







