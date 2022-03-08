from typing import Dict, List
from gensim.models import Doc2Vec
from gensim.summarization.textcleaner import tokenize_by_word
from gensim.models.doc2vec import TaggedDocument

from src.pipeline.text_preprocessing import run_preprocessing_steps


def _tag_training_corpus(train_corpus: List[List[str]]):
    """ Create TaggedDocument objects for the training corpus 
        args:
            train_corpus: A list of documents. Each documents should be a list of tokens
    """

    # TODO: convert to a generator to handle bigger datasets
    tagged_list = list()
    for i, d in enumerate(train_corpus):
        tagged_list.append(TaggedDocument(d, [i]))

    return tagged_list 


def prepare_documents(bills: List, preprocessing_steps: List, train=False):
    """ Given a list of bills, preprocess and prepare them for training doc vectors
        Args:
            bills: The list of bills to be prepared
            preprocessing_steps: preprocessing steps to be applied to the bills
            train: Whether the bills are the training corpus or not
        Return:
            If train, a list of TaggedDocuments
            If not train, a list of tokens per documents 
    """

    bills_preproc = run_preprocessing_steps(bills, preprocessing_steps)

    tokenized_bills = [list(tokenize_by_word(x)) for x in bills_preproc]

    if train:
        ret_corpus = _tag_training_corpus(tokenized_bills)
    else:
        ret_corpus = tokenized_bills

    return ret_corpus


def train_model(train_corpus: List[TaggedDocument], hyperparameters: Dict):
    """ Train a doc2vec model
        args:
            training_corpus: Set of bills to train the model
            hyperparameters: The dictionary of the hyperparameters
        return:
            trained Doc2Vec model
    """

    model = Doc2Vec(train_corpus, **hyperparameters)

    # model.build_vocab(train_corpus)

    model.train(train_corpus, total_examples=model.corpus_count, epochs=model.epochs)

    return model


def predict_vectors(test_set: List[List[str]], model: Doc2Vec):
    """ Generate the vectors for the test documents
        args:
            test_set: The List of documents (each document is a list of tokens)
            model: trained doc2vec model

        return:
            inferred vectors for the test set
    """

    vectors = list()

    for doc in test_set:
        inferred_doc = model.infer_vector(doc)

        vectors.append(inferred_doc)

    return vectors








