import pandas as pd
import seaborn as sns
import logging
import matplotlib.pyplot as plt
import numpy as np

from src.utils.general import get_db_conn
from src.utils.eda import retrieve_reproductive_rights_bills, retrieve_bill_metadata

sns.set_style("white")

db_conn = get_db_conn("../../../conf/local/credentials.yaml")
years_of_study = list(np.arange(2015, 2021))


def _categorize_length_label(df):
    """
    Adds a label for the length of the bill
    :param df: dataframe
    :return: dataframe with the column size_category added
    """
    df['size_category'] = "<50k"

    df['size_category'] = np.where(
        ((df.text_size >= 50000) &
         (df.text_size < 100000)), ">=50k <100k ", np.where(
            df.text_size >= 100000, ">=100k", df.size_category))

    return df


def country_level_viz(df):
    """
    Creates the plot for reproductive rights bills for length distribution at country level
    :param df: Dataframe with lenghts
    :return:
    """
    aux = df[df.text_size < 500000]

    plt.clf()
    g = sns.distplot(aux.text_size, bins=50, kde=False)
    xlabels = ['{:,.2f}'.format(x) + 'K' for x in g.get_xticks() / 1000]
    ylabels = ['{:,.0f}'.format(y) + 'K' for y in g.get_yticks() / 1000]
    g.set_xticklabels(xlabels, rotation=90)
    g.set_yticklabels(ylabels, rotation=90)
    g.set_title("Bill size distribution, country level")
    g.set_xlabel("# words per bill")
    g.set_ylabel("# bills")

    g.figure.tight_layout()
    g.figure.savefig("images/eda_5_1_freq.png")


def country_level_over_time_viz(df):
    """
    Creates the plot for visualizing the distribution of bill length at level country overt time
    :param df: Dataframe with metadata of the bills, incluiding the state
    :return:
    """
    total_rep_bills = df.groupby(["session_year_start"], as_index=False)['bill_id'] \
        .count() \
        .rename(columns={'session_year_start': "year_start", "bill_id": "count"})

    plt.clf()
    g = sns.FacetGrid(col_wrap=6,
                      col="session_year_start",
                      data=df[(df.text_size < 200000) & (df.session_year_start.isin(years_of_study))])
    g = (g.map(plt.hist, "text_size", bins=30, color="b").set(
        xlim=(0, 200000),
        xticklabels=['0k', '50k', '100k', '150k', '200k'],
        yticklabels=['0k', '1k', '2k', '3k', '4k'],
        xlabel='Text size',
        ylabel="# of bills"))

    for ax in g.axes.flatten():
        ax.tick_params(labelbottom=True)

    g.fig.tight_layout()
    g.fig.savefig("images/eda_5_3_freq.png")


def length_category_viz(df):
    """
    Create the plot for visualizating the categories of bill length
    :param df: DataFrame with some metadata of bills
    :return:
    """
    _categorize_length_label(df)

    # group bills by size category
    reproductive_rights_bills_df = df.groupby(['size_category'], as_index=False)['bill_id'] \
        .count() \
        .rename(columns={'bill_id': 'count'})

    reproductive_rights_bills_df['prop'] = round(
        reproductive_rights_bills_df['count'] / reproductive_rights_bills_df['count'].sum(), 2)

    reproductive_rights_bills_df = reproductive_rights_bills_df.sort_values(by="prop", ascending=False)

    plt.clf()
    g = sns.barplot(x="prop", y="size_category", data=reproductive_rights_bills_df)
    g.set_title("Bill size distribution, country level")
    g.set_ylabel("# words per bill")
    g.set_xlabel("% of bills")

    g.figure.tight_layout()
    g.figure.savefig("images/eda_5_1_prop.png")


def state_level_viz(df):
    """
    Creates the plot for visualizing the bill length at state level
    :param df: Dataframe with metadata of the bill, including state
    :return:
    """
    reproductive_rights_bills_data_viz = df.copy()

    reproductive_rights_bills_data_viz['labels'] = reproductive_rights_bills_data_viz.text_size.apply(
        lambda x: str(round(x / 1000, 2)) + "K")

    reproductive_rights_bills_data_viz = reproductive_rights_bills_data_viz.sort_values(by="state")

    plt.clf()
    g = sns.FacetGrid(col_wrap=5, col="state",
                      data=reproductive_rights_bills_data_viz[reproductive_rights_bills_data_viz.text_size < 200000],
                      sharey=False)
    g = (g.map(plt.hist, "text_size", bins=30, color="b").set(
        xlim=(0, 200000),
        xticklabels=['0k', '50k', '100k', '150k', '200k'],
        # yticklabels=['0k','1k','2k','3k','4k'],
        xlabel='Text size',
        ylabel="# of bills"))

    for ax in g.axes.flatten():
        ax.tick_params(labelbottom=True)

    g.fig.tight_layout()
    g.fig.savefig("images/eda_5_2_freq.png")


def state_level_overt_time_viz(df):
    """
    Create the plot for vizualizing the bill length per state over the last n years
    :param df: Dataframe with bill meta data info including state
    :return:
    """
    reproductive_bills_state = df.sort_values(by="state")

    plt.clf()
    g = sns.FacetGrid(row="state", col="session_year_start",
                      data=reproductive_bills_state[(reproductive_bills_state.text_size < 200000)
                                                    & (reproductive_bills_state.session_year_start.isin(
                          years_of_study))],
                      margin_titles=True, sharey=False)
    g = (g.map(plt.hist, "text_size", bins=10, color="b").set(
        xlim=(0, 200000),
        xticklabels=['0k', '50k', '100k', '150k', '200k'],
        # yticklabels=['0k','1k','2k','3k','4k'],
        xlabel='Text size',
        ylabel="# of bills"))

    for ax in g.axes.flatten():
        ax.tick_params(labelbottom=True)

    g.fig.tight_layout()
    g.fig.savefig("images/eda_5_3_freq_states.png")


reproductive_rights_bills = retrieve_reproductive_rights_bills(db_conn)
# visualization for bills length at country level
country_level_viz(reproductive_rights_bills)

# visualization for bills length categorizing the length
length_category_viz(reproductive_rights_bills)

# visualization for bills length at country level
reproductive_bills_state = retrieve_bill_metadata(db_conn, reproductive_rights_bills)
state_level_viz(reproductive_bills_state)

# visualization for bills length country level over time
country_level_over_time_viz(reproductive_bills_state)

# visualization for bills length state level overt time
state_level_overt_time_viz(reproductive_bills_state)
