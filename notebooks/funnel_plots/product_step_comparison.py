"""Visualization for ProductSelection abandonment step comparisons."""

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle


def _prepare_data(df, step_order):
    """Pivot data and calculate percentages."""
    pivot = df.pivot(index='abandonproductsteptype', columns='departureyear', values='count').fillna(0)

    # Extract progressed row before reindexing
    prog_counts = pivot.loc[pivot.index.isna()].iloc[0] if pivot.index.isna().any() else None

    # Get only the step rows
    pivot_steps = pivot[pivot.index.notna()].reindex(step_order, fill_value=0)

    # Calculate totals including progressed
    totals = pivot.sum(axis=0)

    # Calculate percentages from total (all reservations)
    pivot_pct = pivot_steps.div(totals, axis=1) * 100
    prog_pct = prog_counts / totals * 100 if prog_counts is not None else None

    return pivot_pct, prog_pct, totals


def _draw_stacked_bars(ax, pivot_pct, colors):
    """Draw stacked horizontal bars for each year."""
    left = {2019: 0, 2024: 0}

    for i, step in enumerate(pivot_pct.index):
        for y_pos, year in enumerate([2019, 2024]):
            pct = pivot_pct.loc[step, year]
            ax.barh(y_pos, pct, left=left[year], height=0.5,
                   label=step if y_pos == 0 else '', color=colors[i], alpha=0.8)

            if pct > 5:
                ax.text(left[year] + pct/2, y_pos, f'{pct:.1f}%',
                       ha='center', va='center', fontsize=9, fontweight='bold')

            left[year] += pct

    return left


def _draw_progressed_bars(ax, prog_pct, left_positions):
    """Draw dashed outline bars for progressed reservations."""
    if prog_pct is None:
        return

    for y_pos, year in enumerate([2019, 2024]):
        if year in prog_pct.index:
            pct = prog_pct[year]
            ax.barh(y_pos, pct, left=left_positions[year], height=0.5,
                   fill=False, edgecolor='steelblue', linewidth=2, linestyle='--')
            if pct > 5:
                ax.text(left_positions[year] + pct/2, y_pos, f'{pct:.1f}%',
                       ha='center', va='center', fontsize=9, fontweight='bold')


def plot_product_step_comparison(df, title):
    """
    Create a stacked horizontal bar chart comparing ProductSelection abandonment steps for 2019 vs 2024.

    Parameters:
    -----------
    df : DataFrame
        Query result with columns: departureyear, abandonproductsteptype, count
    title : str
        Chart title
    """
    step_order = ['departure', 'hotel', 'room', 'flight', 'ship', 'cabin']
    pivot_pct, prog_pct, totals = _prepare_data(df, step_order)

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = plt.cm.Pastel1(np.linspace(0, 1, len(step_order))) # pyright: ignore[reportAttributeAccessIssue]

    left_positions = _draw_stacked_bars(ax, pivot_pct, colors)
    _draw_progressed_bars(ax, prog_pct, left_positions)

    # Configure axes
    ax.invert_yaxis()
    ax.set_yticks([0, 1])
    ax.set_yticklabels([f'2019 (n={int(totals[2019]):,})', f'2024 (n={int(totals[2024]):,})'])
    ax.set_xlabel('Percentage of ProductSelection Abandonments')
    ax.set_title(title)
    ax.set_xlim(0, 100)

    # Add legend
    handles, labels = ax.get_legend_handles_labels()
    prog_patch = Rectangle((0, 0), 1, 1, fill=False, edgecolor='steelblue', linewidth=2, linestyle='--')
    ax.legend(handles + [prog_patch], labels + ['Progressed'],
             loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=len(step_order)+1, frameon=False)

    fig.tight_layout()
    plt.show()
