import pandas as pd
import seaborn as sns
import logging
import matplotlib.pyplot as plt
import numpy as np

from src.utils.general import get_db_conn
from src.utils.eda import retrieve_states_catalogue, retrieve_reproductive_rights_bills, retrieve_bill_metadata

sns.set_style("white")

db_conn = get_db_conn("../../../conf/local/credentials.yaml")
years_of_study = list(np.arange(2015,2021))
bill_status_color = {'Passed': 'green', "Other": 'blue'}
status_catalogue = pd.DataFrame({"status_id": [1, 2, 3, 4, 5, 6],
                                 "status": ["Introduced", "Engrossed", "Enrolled", "Passed", "Vetoed", "Failed"]})


def _create_status_df(db_conn):
    """
    Creates a dataframe that contains the status of each bill
    :param db_conn: Connection to database
    :return:
    """
    reproductive_rights_bills = retrieve_reproductive_rights_bills(db_conn)
    bill_status = retrieve_bill_metadata(db_conn, reproductive_rights_bills)

    bill_status_desc = bill_status.merge(status_catalogue, how="inner",
                                         left_on="status", right_on="status_id") \
        .rename(columns={'status_x': "status_code", "status_y": "status"})
    bill_status_desc['status_passed'] = np.where(bill_status_desc.status == 'Passed',
                                                 "Passed", "Other")

    return bill_status_desc


def country_level_viz(df):
    """
    Creates the plot for the country level analysis on bills statuses
    :param df: Dataframe with data of the bills
    :return:
    """
    bill_status_df = df.groupby(['status_passed'], as_index=False)['state'] \
        .count() \
        .rename(columns={'state': "count"})

    bill_status_df['prop'] = round(bill_status_df['count'] / bill_status_df['count'].sum(), 2)

    bill_status_df['color'] = bill_status_df.status_passed.apply(lambda x: bill_status_color[x])

    plt.clf()
    g = sns.barplot(x="prop", y="status_passed", palette=bill_status_df.color,
                    data=bill_status_df)
    g.set_title("Bill status, country level")
    g.set_ylabel("bill status")
    g.set_xlabel("% of bills")

    g.figure.tight_layout()
    g.figure.savefig("images/eda_6_1_prop.png")


def country_level_over_time(df):
    """
    Creates the plot for country level over time analysis on bills statuses
    :param df: Dataframe with data of the bills
    :return:
    """
    bill_status_time = df.groupby(['session_year_start', 'status_passed'], as_index=False)['state'] \
        .count() \
        .rename(columns={'state': "count"})

    bill_status_time['prop'] = bill_status_time.groupby(['session_year_start'], as_index=False)['count'] \
        .transform(lambda x: round(x / x.sum(), 2)) \
        .reset_index() \
        .rename(columns={'count': 'prop'})['prop']

    bill_status_time['color'] = bill_status_time.status_passed.apply(lambda x: bill_status_color[x])

    plt.clf()
    g = sns.FacetGrid(col="session_year_start",
                      data=bill_status_time[bill_status_time.session_year_start.isin(years_of_study)],
                      sharex=False, hue="status_passed",
                      palette=bill_status_time.color)
    g = (g.map(plt.bar, "status_passed", "prop").set(
        xlabel='bill status',
        ylabel="% of bills"))
    g.set_xticklabels(rotation=90)

    for ax in g.axes.flatten():
        ax.tick_params(labelbottom=True)

    g.fig.tight_layout()
    g.fig.savefig("images/eda_6_3_prop_year.png")


def state_level_viz(db_conn, df):
    """
    Creates the plot for state level analysis on bills statuses
    :param db_conn: Connection to database
    :param df: Dataframe with data of the bills
    :return:
    """
    bill_status_state_df = df.groupby(['state', 'status_passed'], as_index=False)['bill_id'] \
        .count() \
        .rename(columns={'bill_id': 'count'})

    bill_status_state_df['prop'] = bill_status_state_df \
        .groupby(["state"], as_index=False)['count'] \
        .transform(lambda x: round(x / x.sum(), 2)) \
        .rename(columns={'count': 'prop'})['prop']

    states_catalogue = retrieve_states_catalogue(db_conn)
    bill_status_state_viz_desc = bill_status_state_df.merge(states_catalogue, how="inner",
                                                            left_on="state", right_on="state_abbreviation") \
        .rename(columns={'state_x': 'state_code', "state_y": "state"})

    bill_status_state_viz_desc['color'] = bill_status_state_viz_desc.status_passed \
        .apply(lambda x: bill_status_color[x])

    bill_status_state_viz_desc = bill_status_state_viz_desc.sort_values(by="state")

    plt.clf()
    g = sns.FacetGrid(col_wrap=5, col="state",
                      data=bill_status_state_viz_desc, sharex=False, hue="status_passed",
                      palette=bill_status_state_viz_desc.color)
    g = (g.map(plt.bar, "status_passed", "prop").set(
        xlabel='bill_ status',
        ylabel="% of bills"))
    g.set_xticklabels(rotation=90)

    for ax in g.axes.flatten():
        ax.tick_params(labelbottom=True)

    g.fig.tight_layout()
    g.fig.savefig("images/eda_6_2_prop.png")


def state_level_over_time_viz(db_conn, df):
    """
    Creates the plot for state level over time analysis on bills statuses
    :param db_conn: Connection to database
    :param df: Dataframe with data of bills
    :return:
    """

    bill_status_state_time = df.groupby(['session_year_start', 'status_passed', "state"], as_index=False)['bill_id'] \
        .count() \
        .rename(columns={'bill_id': "count"})

    bill_status_state_time['prop'] = bill_status_state_time.groupby(['state', 'session_year_start'],
                                                                    as_index=False)['count'] \
        .transform(lambda x: round(x / x.sum(), 2)) \
        .rename(columns={'count': 'prop'})['prop']

    states_catalogue = retrieve_states_catalogue(db_conn)
    bill_status_state_time_viz_desc = bill_status_state_time.merge(states_catalogue, how="inner",
                                                                   left_on="state", right_on="state_abbreviation") \
        .rename(columns={'state_x': "state_code", "state_y": "state"}) \
        .sort_values(by="state")

    bill_status_state_time_viz_desc['color'] = bill_status_state_time_viz_desc.status_passed \
        .apply(lambda x: bill_status_color[x])

    plt.clf()
    g = sns.FacetGrid(row="state", col="session_year_start",
                      data=bill_status_state_time_viz_desc[
                          bill_status_state_time_viz_desc.session_year_start.isin(years_of_study)],
                      hue="status_passed", palette=bill_status_state_time_viz_desc.color,
                      sharex=False, margin_titles=True)
    g = (g.map(plt.bar, "status_passed", "prop").set(
        xlabel='bill status',
        ylabel="% of bills"))
    g.set_xticklabels(rotation=90)

    for ax in g.axes.flatten():
        ax.tick_params(labelbottom=True)

    g.fig.tight_layout()
    g.fig.savefig("images/eda_6_3_prop_state_year.png")


bill_status_desc = _create_status_df(db_conn)
# country level of passed bills visualization
country_level_viz(bill_status_desc)

# state level of passed bills visualization
state_level_viz(db_conn, bill_status_desc)

# country level of passed bills over time visualization
country_level_over_time(bill_status_desc)

# state level of passed bills over time visualization
state_level_over_time_viz(db_conn, bill_status_desc)
