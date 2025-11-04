"""Visualization for overall reservation completion comparison."""

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


def human_format(num, pos):
    """Format numbers as humanized strings (e.g., 20k, 1.5M)."""
    if num >= 1_000_000:
        return f'{num/1_000_000:.1f}M'
    elif num >= 1_000:
        return f'{num/1_000:.0f}k'
    else:
        return f'{num:.0f}'


def plot_completion_comparison(df):
    """
    Create dual-axis bar chart comparing total reservations vs confirmed reservations by year.

    Parameters:
    -----------
    df : DataFrame
        Query result with columns: departureyear, total_reservations, confirmed
    """
    # Visualization
    fig, ax1 = plt.subplots(figsize=(12, 6))

    x = np.arange(len(df['departureyear']))
    width = 0.35

    # Left y-axis for total_reservations
    ax1.set_xlabel('Departure Year')
    ax1.set_ylabel('Total Reservations', color='tab:blue')
    bars1 = ax1.bar(x - width/2, df['total_reservations'], width, label='Total Reservations', color='tab:blue', alpha=0.7)
    ax1.tick_params(axis='y', labelcolor='tab:blue')
    ax1.set_xticks(x)
    ax1.set_xticklabels(df['departureyear'])

    # Set left y-axis range and ticks
    ax1.set_ylim(0, 1_100_000)
    ax1.set_yticks(np.arange(0, 1_000_001, 200_000))

    # Format left y-axis with humanized numbers
    ax1.yaxis.set_major_formatter(FuncFormatter(human_format))

    # Right y-axis for confirmed
    ax2 = ax1.twinx()
    ax2.set_ylabel('Confirmed', color='tab:green')
    bars2 = ax2.bar(x + width/2, df['confirmed'], width, label='Confirmed', color='tab:green', alpha=0.7)
    ax2.tick_params(axis='y', labelcolor='tab:green')

    # Set right y-axis range and ticks
    ax2.set_ylim(0, 55_000)
    ax2.set_yticks(np.arange(0, 50_001, 10_000))

    # Format right y-axis with humanized numbers
    ax2.yaxis.set_major_formatter(FuncFormatter(human_format))

    # Add percentage labels above confirmed bars
    for i, (total, confirmed) in enumerate(zip(df['total_reservations'], df['confirmed'])):
        percentage = (confirmed / total * 100) if total > 0 else 0
        ax2.text(x[i] + width/2, confirmed, f'{percentage:.1f}%',
                 ha='center', va='bottom', fontsize=9, color='darkgreen')

    # Title and legend
    plt.title('Reservations by Departure Year: Total vs Confirmed')
    fig.legend(loc='upper right', bbox_to_anchor=(0.9, 0.9))
    fig.tight_layout()
    plt.show()
