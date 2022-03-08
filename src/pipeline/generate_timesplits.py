from typing import List, Dict
from triage.component.timechop import Timechop
from triage.component.timechop.plotting import visualize_chops


def get_time_splits(temporal_config: Dict, visualize=True, figure_path=None) -> List[Dict]:
    """Given the temporal config, using Timechop, get the time blocks and as_of_dates
        The temporal config dictionary should contain:
            - feature_start_time
            - feature_end_time
            - label_start_time
            - label_end_time
            - model_update_frequency
            - max_training_histories
            - test_durations
            - training_as_of_date_frequencies
            - test_as_of_date_frequencies
            - label_timespans
    """
    chopper = Timechop(
        feature_start_time=temporal_config['feature_start_time'],
        feature_end_time=temporal_config['feature_end_time'],
        label_start_time=temporal_config['label_start_time'],
        label_end_time=temporal_config['label_end_time'],
        model_update_frequency=temporal_config['model_update_frequency'],
        training_as_of_date_frequencies=temporal_config['training_as_of_date_frequencies'],
        max_training_histories=temporal_config['max_training_histories'],
        training_label_timespans=temporal_config['label_timespans'],
        test_as_of_date_frequencies=temporal_config['test_as_of_date_frequencies'],
        test_durations=temporal_config['test_durations'],
        test_label_timespans=temporal_config['label_timespans'],
    )

    result = chopper.chop_time()

    if visualize:
        if figure_path is None:
            figure_path = 'timechop.png'

        visualize_chops(
            chopper=chopper,
            show_as_of_times=True,
            show_boundaries=True,
            save_target=figure_path
        )

    return result
