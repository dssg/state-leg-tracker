import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from descriptors import cachedproperty

from triage.component.postmodeling.contrast.model_evaluator import ModelEvaluator



"""This script extends the capabilities of triage's postmodeling ModelEvaluator Class"""




class ModelEvaluatorACLU(ModelEvaluator):

    @cachedproperty
    def metadata(self):
        return next(self.engine.execute(
            f'''select
                    m.model_id,
                    m.model_group_id,
                    m.hyperparameters,
                    m.model_hash,
                    m.train_end_time,
                    m.train_matrix_uuid,
                    m.training_label_timespan,
                    m.model_type,
                    mg.model_config,
                    p.matrix_uuid
                FROM triage_metadata.models m
                JOIN triage_metadata.model_groups mg
                USING (model_group_id)
                JOIN test_results.prediction_metadata p 
                USING (model_id)                
                WHERE model_group_id = {self.model_group_id}
                AND model_id = {self.model_id}
            ''')
        )


    def plot_feature_distribution_with_score_bands(self, path, score_bands, feature_list=None, score_band_subsets=None):
        """ 
        plot the feature distributions and compare them across given score bands 
        
        Args:
            path (str): project path,
            score_bands (Dict[str:List[float]]): The score bands to consider (likelihood bins)
            feature_list (List[str]): List of features to plot
            score_band_subsets (Dict[str: List[str]]): The subsets of bands to compare
        """

        if feature_list is None:
            f_importances = self.feature_importances(path)
            top_f = f_importances[f_importances['rank_abs'] <= 10]['feature'].tolist()
            feature_list = top_f
        
        n = len(feature_list)
        c = len(score_band_subsets.keys())

        fig, axs = plt.subplots(n, c, figsize=(5*c,5*n))
        axs = axs.reshape(-1)

        matrix = self.preds_matrix(path=path)

        # splitting the matrix into the given score bands
        split_matrices = dict()

        for band_label, limits in score_bands.items():
            msk = (matrix.score >= limits[0]) & (matrix.score < limits[1])
            split_matrices[band_label] = matrix[msk]

        for i, feature in enumerate(feature_list):
            for j, subset_name in enumerate(score_band_subsets):
                bands = score_band_subsets[subset_name]
                plt_idx = c * i + j
                ax = axs[plt_idx]
                for band in bands:
                    sns.distplot(
                        split_matrices[band][feature],
                        hist=False,
                        kde=True,
                        kde_kws={'linewidth': 2},
                        ax=ax,
                        label=band
                    )
                m = matrix[feature].max()
                ax.set_xlim(0, m)
                ax.legend()
                sns.despine()
                    
            





