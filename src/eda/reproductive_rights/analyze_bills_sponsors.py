import psycopg2
import yaml
import pandas as pd
import seaborn as sns
import multiprocessing
import matplotlib.pyplot as plt
import numpy as np

from src.utils.general import get_db_conn
from src.utils.eda import retrieve_states_catalogue, retrieve_reproductive_rights_bills, retrieve_bill_metadata

sns.set_style("white")

db_conn = get_db_conn("../../../conf/local/credentials.yaml")
years_of_study = list(np.arange(2015,2021))
party_catalogue = {'R': 'Republican', "D": "Democrat", 'I': 'Independent', "N": "Nonpartisan",
                  "L": "Libertarian", "G": "Green Party"}
party_color = {'R': "r", "D": "b", "G": "g", "L": "orange", "N": "cyan", "I": "purple", "X": "pink"}



def _retrieve_sponsors_data(db_conn):
    """
    Retrieves the sponsors for each bill
    :param db_conn: Connection to database
    :return:
    """

    q = """
    select bill_id, party
    from temp_eda.sponsors_data
    """

    sponsors_data = pd.read_sql(q, db_conn)
    return sponsors_data



def _create_df_sponsors(db_conn):
    """
    Creates the dataframe that has sponsors and metadata from bills
    :param db_conn: Connection to database
    :return:
    """
    reproductive_rights_bills = retrieve_reproductive_rights_bills(db_conn)
    all_data = retrieve_bill_metadata(db_conn, reproductive_rights_bills)
    all_data['bill_id'] = all_data.bill_id.astype(int)

    df = all_data.merge(reproductive_rights_bills, how="inner", on="bill_id")

    sponsors_df = _retrieve_sponsors_data(db_conn)
    sponsors_df['bill_id'] = sponsors_df['bill_id'].astype(int)
    all_bill_info_df = df.merge(sponsors_df, how="inner", on="bill_id")

    return all_bill_info_df


def _create_party_df():
    """
    Retrieves the party dataframe
    :return:
    """

    party_df = pd.DataFrame.from_dict(party_catalogue, orient="index", columns=["description"])
    party_df['party'] = party_df.index

    return party_df


def _sponsorship(proportion, party):
    """
    Adds the sponsorship label of a bill with respect to the party proportion of a bill
    :param proportion: Proportion of this party in this bill
    :param party: The party to anlayze
    :return:
    """

    if proportion < 0.5:
        return "minority " + party_catalogue[party].lower()
    elif proportion == 0.5:
        return "bipartisan "
    elif proportion > 0.5:
        return "majority " + party_catalogue[party].lower()


def _add_party(sponsorship):
    """
    Adds a column with a letter related to the party this bill had been sponsored by
    :param sponsorship: The role of the party in the role
    :return:
    """

    if sponsorship.endswith('republican'):
        return "R"
    elif sponsorship.endswith("democrat"):
        return "D"
    elif sponsorship.endswith("libertarian"):
        return "L"
    elif sponsorship.endswith("nonpartisan"):
        return "N"
    elif sponsorship.endswith("independent"):
        return "I"
    elif sponsorship.startswith("bipartisan"):
        return "X"
    else:
        return "G"


def _retrieve_max_prop_bill(db_conn, df):
    """
    Identifies the proportion for each party in a bill
    :param db_conn: Connection to database
    :param df: Dataframe with data for the bills
    :return:
    """

    bill_sponsors_party = df.groupby(['state', 'bill_id', 'party', 'session_year_start'], as_index=False)['session_id'] \
        .count() \
        .rename(columns={'session_id': 'count_party'})

    bill_sponsors_party['prop'] = bill_sponsors_party.groupby(['bill_id'], as_index=False)['count_party'] \
        .transform(lambda x: round(x / x.sum(), 2)) \
        .rename(columns={'count_party': "prop"})['prop']

    states_catalogue = retrieve_states_catalogue(db_conn)
    bill_sponsors_party_state = bill_sponsors_party.merge(states_catalogue, how="inner",
                                                          left_on="state", right_on="state_abbreviation") \
        .rename(columns={'state_x': "state_abbr", "state_y": "state"})

    max_prop_per_bill = bill_sponsors_party_state.groupby(['state', 'bill_id'], as_index=False)[['prop', 'party']] \
        .max()

    return (bill_sponsors_party, max_prop_per_bill)


def country_level_viz_approach_1(db_conn, df):
    """
    Creates the plot for country level analysis of sponsors per bill
    :param db_conn: Connection to database
    :param df: Dataframe with data for the bill
    :return:
    """
    freq_party = df.groupby(['party'], as_index=False) \
        .agg({'prop': 'count'}) \
        .rename(columns={'prop': 'count'}) \
        .sort_values(by="count", ascending=False)

    party_df = _create_party_df()

    freq_party_labels = freq_party.merge(party_df, how="inner", on="party")

    freq_party_labels['prop'] = round(freq_party_labels['count'] / freq_party_labels['count'].sum(), 2)
    freq_party_labels['color'] = freq_party_labels['party'].apply(lambda x: party_color[x])

    plt.clf()
    g = sns.barplot(x="prop", y="description", data=freq_party_labels,
                    palette=freq_party_labels['color'])
    g.set_ylabel("party")
    g.set_xlabel("%")
    g.set_title("Bill sponsor, country level")

    g.figure.tight_layout()
    g.figure.savefig("images/eda_4_1_prop.png")

    return freq_party


def state_level_viz_approach_1(db_conn, df):
    """
    Create the plot for state level analysis of sponsors bill
    :param db_conn: Connection to database
    :param df: Dataframe with data for the bill
    :return:
    """
    freq_party = df.groupby(['state', 'party'], as_index=False) \
        .agg({'prop': 'count'}) \
        .rename(columns={'prop': 'count'}) \
        .sort_values(by="state")

    freq_party['prop'] = freq_party.groupby(['state'], as_index=False) \
        .apply(lambda x: round(x['count'] / x['count'].sum(), 2))['count']

    party_df = _create_party_df()

    freq_party_labels = freq_party.merge(party_df, how="inner", on="party")
    freq_party_labels['color'] = freq_party_labels['party'].apply(lambda x: party_color[x])

    freq_party_labels = freq_party_labels.sort_values(by="state")

    plt.clf()
    g = sns.FacetGrid(col_wrap=5, col="state",
                      data=freq_party_labels, hue="party",
                      palette=party_color, sharex=False)
    g = (g.map(plt.bar, "description", "prop").set(
        xlabel='party',
        ylabel="%"))
    g.set_xticklabels(rotation=90)

    for ax in g.axes.flatten():
        ax.tick_params(labelbottom=True)

    g.fig.tight_layout()
    g.fig.savefig("images/eda_4_2_prop.png")


def country_level_over_time_viz_approach_1(df):
    """
    Creates the plot for country level over time analysis of bills sponsors
    :param df: Dataframe with data of the bill
    :return:
    """
    bill_sponsors_party_time = df \
        .groupby(['state', 'bill_id', 'party', 'session_year_start'], as_index=False)['count_party'] \
        .count()

    bill_sponsors_party_time['prop'] = bill_sponsors_party_time \
        .groupby(['state', 'bill_id', 'session_year_start'], as_index=False)['count_party'] \
        .transform(lambda x: round(x / x.sum(), 2)) \
        .rename(columns={'count_party': 'prop'})['prop']

    max_bill_party_time = bill_sponsors_party_time.groupby(['state', "bill_id", "session_year_start"], as_index=False)[
        ['prop', "party"]] \
        .max()

    bill_party_time_df = max_bill_party_time.groupby(['party', "session_year_start"], as_index=False)['state'] \
        .count() \
        .rename(columns={'state': 'count'})

    party_df = _create_party_df()
    bill_party_time_desc = bill_party_time_df.merge(party_df, how="inner", on="party")
    bill_party_time_desc.head()

    bill_party_time_desc['prop'] = bill_party_time_desc.groupby(["session_year_start"], as_index=False)['count'] \
        .transform(lambda x: round(x / x.sum(), 2)) \
        .rename(columns={'count': 'prop'})['prop']

    plt.clf()
    g = sns.FacetGrid(col_wrap=5, col="session_year_start",
                      data=bill_party_time_desc[bill_party_time_desc.session_year_start.isin(years_of_study)],
                      hue="party",
                      palette=party_color, sharex=False)
    g = (g.map(plt.bar, "description", "prop").set(
        xlabel='party',
        ylabel="% of bills"))
    g.set_xticklabels(rotation=90)

    for ax in g.axes.flatten():
        ax.tick_params(labelbottom=True)

    g.fig.tight_layout()
    g.fig.savefig("images/eda_4_3_year.png")

    return max_bill_party_time


def state_level_over_time_viz_approach_1(db_conn, df):
    """
    Creates the plot for state level over time analysis for bills sponsors
    :param db_conn: Connection to database
    :param df: Dataframe with data of the bills
    :return:
    """
    bill_party_state_time = max_bill_party_time.groupby(['state', 'party', "session_year_start"],
                                                        as_index=False)['bill_id'] \
        .count() \
        .rename(columns={'bill_id': 'count'})

    bill_party_state_time['prop'] = bill_party_state_time.groupby(['state', 'session_year_start'],
                                                                  as_index=False)['count'] \
        .transform(lambda x: round(x / x.sum(), 2)) \
        .rename(columns={'count': 'prop'})['prop']

    states_catalogue = retrieve_states_catalogue(db_conn)
    bill_party_state_time_desc = bill_party_state_time.merge(states_catalogue, how="inner", left_on="state",
                                                             right_on="state_abbreviation") \
        .rename(columns={'state_x': "state_code", "state_y": "state"})

    party_df = _create_party_df()
    bill_party_state_time_df = bill_party_state_time_desc.merge(party_df, how="inner", on="party")

    bill_party_state_time_df = bill_party_state_time_df.sort_values(by="state")

    plt.clf()
    g = sns.FacetGrid(row="state", col="session_year_start",
                      data=bill_party_state_time_df[bill_party_state_time_df.session_year_start.isin(years_of_study)],
                      hue="party", palette=party_color, sharex=False, margin_titles=True)
    g = (g.map(plt.bar, "description", "prop").set(
        xlabel='party',
        ylabel="% of bills"))
    g.set_xticklabels(rotation=90)

    for ax in g.axes.flatten():
        ax.tick_params(labelbottom=True)

    g.fig.tight_layout()
    g.fig.savefig("images/eda_4_3_year_state.png")


def country_level_viz_approach_2(df):
    """
    Creates the plot for country level analysis on bills sponsors with approach 2
    :param df: Dataframe with data of the bills
    :return:
    """
    party_df = _create_party_df()
    approach_2_df = df.merge(party_df, how="inner", on="party")

    approach_2_df['sponsorship'] = approach_2_df.apply(lambda x: _sponsorship(x['prop'], x['party']), axis=1)
    approach_2_unique_bills = approach_2_df[approach_2_df.sponsorship.str.startswith("majority") |
                                            (approach_2_df.sponsorship.str.startswith("bipartisan"))]

    approach_2_country_level = approach_2_unique_bills.groupby(['sponsorship'], as_index=False)['state'] \
        .count() \
        .rename(columns={'state': 'count'})

    approach_2_country_level['party'] = approach_2_country_level \
        .apply(lambda x: _add_party(x['sponsorship']), axis=1)

    approach_2_country_level['prop'] = round(
        approach_2_country_level['count'] / approach_2_country_level['count'].sum(), 2)

    approach_2_country_level['color'] = approach_2_country_level.apply(lambda x: party_color[x['party']], axis=1)

    approach_2_country_level = approach_2_country_level.sort_values(by="prop", ascending=False)

    a = approach_2_country_level[approach_2_country_level.prop > 0]

    plt.clf()
    g = sns.barplot(x="prop", y="sponsorship", data=a, palette=a['color'])
    g.set_xlabel("% of bills")
    g.set_title("Bill sponsor, country level")

    g.figure.tight_layout()
    g.figure.savefig("images/eda_4_1_b_prop.png")

    return approach_2_unique_bills


def state_level_viz_approach_2(db_conn, df):
    """
    Creates the state level plot with approach 2
    :param db_conn: Connection to database
    :param df: Dataframe with data of the bills
    :return:
    """
    approach_2_state = df.groupby(['sponsorship', 'state'], as_index=False)['bill_id'] \
        .count() \
        .rename(columns={'bill_id': 'count'})

    approach_2_state['party'] = approach_2_state \
        .apply(lambda x: _add_party(x['sponsorship']), axis=1)

    states_catalogue = retrieve_states_catalogue(db_conn)
    approach_2_state_desc = approach_2_state.merge(states_catalogue, how="inner",
                                                   left_on="state", right_on="state_abbreviation") \
        .rename(columns={'state_x': "state_code", "state_y": "state"})

    approach_2_state_desc['prop'] = approach_2_state_desc.groupby(['state'], as_index=False)['count'] \
        .transform(lambda x: round(x / x.sum(), 2)) \
        .rename(columns={'count': 'prop'})['prop']

    approach_2_state_desc['color'] = approach_2_state_desc.apply(lambda x: party_color[x['party']], axis=1)

    approach_2_state_desc = approach_2_state_desc.sort_values(by=["state", "prop"], ascending=[True, False])

    plt.clf()
    g = sns.FacetGrid(col_wrap=5, col="state", data=approach_2_state_desc, hue="party",
                      palette=party_color, sharex=False, height=4)
    g = (g.map(plt.bar, "sponsorship", "prop").set(
        xlabel='party',
        ylabel="% of bills"))
    g.set_xticklabels(rotation=90)

    for ax in g.axes.flatten():
        ax.tick_params(labelbottom=True, rotation=90)

    g.fig.tight_layout()
    g.fig.savefig("images/eda_4_2_b_prop.png")


def country_level_over_time_viz_approach_2(df):
    """
    Creates the plot for country level overt time analysis with approach 2
    :param df: Dataframe with data of the bills
    :return:
    """
    approach_2_country_time = df.groupby(['sponsorship', 'session_year_start'], as_index=False)['bill_id'] \
        .count() \
        .rename(columns={'bill_id': 'count'})

    approach_2_country_time['party'] = approach_2_country_time \
        .apply(lambda x: _add_party(x['sponsorship']), axis=1)

    approach_2_country_time['prop'] = approach_2_country_time.groupby(['session_year_start'], as_index=False)['count'] \
        .transform(lambda x: round(x / x.sum(), 2)) \
        .rename(columns={'count': 'prop'})['prop']

    approach_2_country_time = approach_2_country_time.sort_values(by=['session_year_start', 'prop'],
                                                                  ascending=[True, False])

    plt.clf()
    g = sns.FacetGrid(col="session_year_start",
                      data=approach_2_country_time[approach_2_country_time.session_year_start.isin(years_of_study)],
                      hue="party",
                      palette=party_color, sharex=False, height=4)
    g = (g.map(plt.bar, "sponsorship", "prop").set(
        xlabel='party',
        ylabel="% of bills"))
    g.set_xticklabels(rotation=90)

    for ax in g.axes.flatten():
        ax.tick_params(labelbottom=True)

    g.fig.tight_layout()
    g.fig.savefig("images/eda_4_3_b_year.png")


def state_level_over_time_viz_approach_2(db_conn, df):
    """
    Creates the plot for state level over time analysis with approach 2
    :param db_conn: Connection to the database
    :param df: Dataframe with data for the bill
    :return:
    """
    approach_2_state_time = df.groupby(['sponsorship', 'session_year_start', 'state'], as_index=False)['bill_id'] \
        .count() \
        .rename(columns={'bill_id': 'count'})

    approach_2_state_time['party'] = approach_2_state_time \
        .apply(lambda x: _add_party(x['sponsorship']), axis=1)

    approach_2_state_time['prop'] = approach_2_state_time.groupby(['session_year_start', 'state'],
                                                                  as_index=False)['count'] \
        .transform(lambda x: round(x / x.sum(), 2)) \
        .rename(columns={'count': 'prop'})['prop']

    states_catalogue = retrieve_states_catalogue(db_conn)
    approach_2_state_time_desc = approach_2_state_time.merge(states_catalogue, how="inner", left_on="state",
                                                             right_on="state_abbreviation") \
        .rename(columns={'state_x': 'state_code', 'state_y': 'state'})

    approach_2_state_time_desc = approach_2_state_time_desc.sort_values(by='state')

    plt.clf()
    g = sns.FacetGrid(col="session_year_start", row="state",
                      data=approach_2_state_time_desc[
                          approach_2_state_time_desc.session_year_start.isin(years_of_study)],
                      hue="party",
                      palette=party_color, sharex=False, height=4, margin_titles=True)
    g = (g.map(plt.bar, "sponsorship", "prop").set(
        xlabel='party',
        ylabel="% of bills"))
    g.set_xticklabels(rotation=90)

    for ax in g.axes.flatten():
        ax.tick_params(labelbottom=True, rotation=90)

    g.fig.tight_layout()
    g.fig.savefig("images/eda_4_3_b_state_year.png")


all_bill_info_df = _create_df_sponsors(db_conn)
# country level visualization approach 1 of bills sponsors
bill_sponsors_party, max_prop_per_bill = _retrieve_max_prop_bill(db_conn, all_bill_info_df)
country_level_viz_approach_1(db_conn, max_prop_per_bill)

# state level visualization approach 1 of bills sponsors
state_level_viz_approach_1(db_conn, max_prop_per_bill)

# country level over time approach 1 of bills sponsors
max_bill_party_time = country_level_over_time_viz_approach_1(bill_sponsors_party)

# state level visualization approach 1 of bills sponsors
state_level_over_time_viz_approach_1(db_conn, max_bill_party_time)

# country level visualization approach 2 of bills sponsors
approach_2_unique_bills = country_level_viz_approach_2(bill_sponsors_party)

# state level visualization approach 2 of bills sponsors
state_level_viz_approach_2(db_conn, approach_2_unique_bills)

# country level over time visualization approach 2 of bills sponsors
country_level_over_time_viz_approach_2(approach_2_unique_bills)

# state level over time visualization approach 2 of bills sponsors
state_level_over_time_viz_approach_2(db_conn, approach_2_unique_bills)
