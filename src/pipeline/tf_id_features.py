import itertools

from sklearn.feature_extraction.text import TfidfVectorizer
from src.pipeline.text_preprocessing import run_preprocessing_steps
from src.utils.general import get_issue_area_configuration


def _get_text_preprocessing_steps():
    """Gets the preprocessing configuration of the text"""
    issue_area_config = get_issue_area_configuration("../issue_classifier/experiment_config/issue_area_config.yaml")
    steps = issue_area_config['text_preprocessing']

    return steps


def _tf_idf(texts, steps):
    """
    Calculates the tf_idf sparse matrix with sklearn
    :param texts: List of lists with texts that makes the corpus
    :return:
    """
    all_text = list(itertools.chain(*texts))
    print("start cleaning text")
    cleaned_text = run_preprocessing_steps(all_text, steps)
    print("cleaning text... ", str(len(all_text)))
    tf = TfidfVectorizer()
    tf_idf_matrix = tf.fit_transform(cleaned_text)
    print("tfidf transformed")
    vocabulary = tf.get_feature_names()

    return tf_idf_matrix, vocabulary


def tf_idf_features(texts):
    """Calculates the TF-IDF of a specific corpus"""
    # look up for the preprocessing text steps to apply to the corpus
    steps = _get_text_preprocessing_steps()
    # calculate TF-IDF matrix
    matrix, vocabulary = _tf_idf(texts, steps)

    return matrix, vocabulary