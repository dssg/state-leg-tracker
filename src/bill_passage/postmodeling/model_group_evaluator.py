import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from descriptors import cachedproperty

from triage.component.postmodeling.contrast.model_group_evaluator import ModelGroupEvaluator

class ModelGroupEvaluatorACLU(ModelGroupEvaluator):
    def __init__(self, engine, model_group_id) -> None:
        self.model_group_id = model_group_id
        self.engine=engine

    @cachedproperty
    def metadata(self):
        q = """
            SELECT
                SELECT m.model_id,
                   m.model_group_id,
                   m.hyperparameters,
                   m.model_hash,
                   m.train_end_time,
                   m.train_matrix_uuid,
                   m.training_label_timespan,
                   m.model_type,
                   mg.model_config
                FROM triage_metadata.models m
                JOIN triage_metadata.model_groups mg
                USING (model_group_id)
                WHERE model_group_id = {model_group_id}
        """.format(self.model_group_id)
        

        meta_data_dict = pd.read_sql(q, self.engine).to_dict('records')

        return meta_data_dict

    # @property
    # def model_id(self):
    #     return [dict_row['model_id'] for dict_row in self.metadata]

    # @property
    # def model_hash(self):
    #     return [dict_row['model_hash'] for dict_row in self.metadata]

    # @property
    # def hyperparameters(self):
    #     return [dict_row['hyperparameters'] for dict_row in self.metadata]

    # @property
    # def train_end_time(self):
    #     return [dict_row['train_end_time'] for dict_row in self.metadata]

    # @property
    # def train_matrix_uuid(self):
    #     return [dict_row['train_matrix_uuid'] for dict_row in self.metadata]

    # @property
    # def training_label_timespan(self):
    #     return [dict_row['training_label_timespan'] for dict_row in self.metadata]

    # @property
    # def model_type(self):
    #     return [dict_row['model_type'] for dict_row in self.metadata]

    # @property
    # def model_config(self):
    #     return [dict_row['model_config'] for dict_row in self.metadata]

    # def plot_metric_over_time(self, metric, param_type, param, df=False, figsize=(12, 16), fontsize=20):


    



    