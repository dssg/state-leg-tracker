#### Kasun edit version of timechop visualization

import matplotlib
matplotlib.use('Agg')
import matplotlib.dates as md
import numpy as np
from triage.util.conf import convert_str_to_relativedelta
import matplotlib.pyplot as plt


MIN_FIG_SIZE = (8, 4)

def visualize_chops(chopper, show_as_of_times=True, show_boundaries=True, save_target=None):
    """Visualize time chops of a given Timechop object using matplotlib

    Args:
        chopper (triage.component.timechop.Timechop) A fully-configured Timechop object
        show_as_of_times (bool, default True) Whether or not to draw horizontal lines
            for as-of-times
        show_boundaries (bool, default True) Whether or not to show a rectangle around matrices
            and dashed lines around feature/label boundaries
        save_target (path or filehandle, default None) A save target for matplotlib to save
            the figure to. Defaults to None, which won't save anything
    """
    chops = chopper.chop_time()

    chops.reverse()

    num_chops = len(chops)

    fig_size = _set_fig_size(num_chops)
    fig, axes = plt.subplots(num_chops, sharex=True, sharey=True, figsize=fig_size)

    for idx, chop in enumerate(chops):

        # Set axis object depending on the number of time chops
        if num_chops > 1:
            ax = axes[idx]
            axes[0].set_title("Timechop: Temporal cross-validation blocks")
        else:
            ax = axes
            ax.set_title("Timechop: Temporal cross-validation blocks")
        
        train_as_of_times = chop["train_matrix"]["as_of_times"]
        test_as_of_times = chop["test_matrices"][0]["as_of_times"]

        test_label_timespan = chop["test_matrices"][0]["test_label_timespan"]
        training_label_timespan = chop["train_matrix"]["training_label_timespan"]

        color_rgb = np.random.random(3)

        if show_as_of_times:
            # Train matrix (as_of_times)
            ax.hlines(
                [x for x in range(len(train_as_of_times))],
                [x.date() for x in train_as_of_times],
                [
                    x.date() + convert_str_to_relativedelta(training_label_timespan)
                    for x in train_as_of_times
                ],
                linewidth=3,
                color=color_rgb,
                label=f"train_{idx}",
            )

            # Test matrix
            ax.hlines(
                [x for x in range(len(test_as_of_times))],
                [x.date() for x in test_as_of_times],
                [
                    x.date() + convert_str_to_relativedelta(test_label_timespan)
                    for x in test_as_of_times
                ],
                linewidth=3,
                color=color_rgb,
                label=f"test_{idx}",
            )

        if show_boundaries:
            # Limits: train
            ax.axvspan(
                chop["train_matrix"]["first_as_of_time"],
                chop["train_matrix"]["last_as_of_time"],
                color=color_rgb,
                alpha=0.3,
            )

            ax.axvline(
                chop["train_matrix"]["matrix_info_end_time"], color="k", linestyle="--"
            )

            # Limits: test
            ax.axvspan(
                chop["test_matrices"][0]["first_as_of_time"],
                chop["test_matrices"][0]["last_as_of_time"],
                color=color_rgb,
                alpha=0.3,
            )

            ax.axvline(
                chop["feature_start_time"], color="k", linestyle="--", alpha=0.2
            )
            ax.axvline(
                chop["feature_end_time"], color="k", linestyle="--", alpha=0.2
            )
            ax.axvline(
                chop["label_start_time"], color="k", linestyle="--", alpha=0.2
            )
            ax.axvline(
                chop["label_end_time"], color="k", linestyle="--", alpha=0.2
            )

            ax.axvline(
                chop["test_matrices"][0]["matrix_info_end_time"],
                color="k",
                linestyle="--",
            )

        ax.yaxis.set_major_locator(plt.NullLocator())
        ax.yaxis.set_label_position("right")
        ax.set_ylabel(f"Block {idx}", rotation="horizontal", labelpad=30)

        ax.xaxis.set_major_formatter(md.DateFormatter("%Y"))
        ax.xaxis.set_major_locator(md.YearLocator())
        ax.xaxis.set_minor_locator(md.MonthLocator())

        # Removing the unnecessary spines around the subplots to reduce clutter
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['top'].set_visible(False)       

    fig.subplots_adjust(hspace=0)
    plt.setp([a.get_xticklabels() for a in fig.axes[:-1]], visible=False)
    plt.tight_layout()
    if save_target:
        plt.savefig(save_target)
    plt.show()


def _set_fig_size(num_chops):
    """Update the figure size based on the number of timechops. 
        Improves the fontsize on annotations

    Args:
        num_chops: Number of time splits    
    """
    width = max(MIN_FIG_SIZE[0], 2*num_chops)
    height = max(MIN_FIG_SIZE[1], num_chops)

    return (width, height)
