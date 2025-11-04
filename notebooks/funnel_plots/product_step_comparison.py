"""Visualization for ProductSelection abandonment step comparisons."""

import numpy as np
import matplotlib.pyplot as plt


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
    # Pivot data and calculate percentages
    pivot_df = df.pivot(index='abandonproductsteptype', columns='departureyear', values='count').fillna(0)
    pivot_pct = pivot_df.div(pivot_df.sum(axis=0), axis=1) * 100

    # Define step order
    step_order = ['departure', 'hotel', 'room', 'flight', 'ship', 'cabin']
    pivot_pct = pivot_pct.reindex(step_order, fill_value=0)

    # Visualization
    fig, ax = plt.subplots(figsize=(12, 6))

    years = [2019, 2024]
    y_pos = np.arange(len(years))

    # Create stacked horizontal bars
    left_2019 = 0
    left_2024 = 0

    colors = plt.cm.Pastel1(np.linspace(0, 1, len(pivot_pct.index))) # pyright: ignore[reportAttributeAccessIssue]

    for i, step in enumerate(pivot_pct.index):
        # 2019 bar (position 0 - top)
        ax.barh(0, pivot_pct.loc[step, 2019], left=left_2019,
                height=0.5, label=step, color=colors[i], alpha=0.8)

        # Add percentage text for larger segments
        pct_val_2019 = pivot_pct.loc[step, 2019]
        if pct_val_2019 > 5:
            ax.text(left_2019 + pct_val_2019/2, 0, f'{pct_val_2019:.1f}%',
                    ha='center', va='center', fontsize=9, fontweight='bold')

        left_2019 += pivot_pct.loc[step, 2019]

        # 2024 bar (position 1 - bottom)
        ax.barh(1, pivot_pct.loc[step, 2024], left=left_2024,
                height=0.5, color=colors[i], alpha=0.8)

        # Add percentage text for larger segments
        pct_val_2024 = pivot_pct.loc[step, 2024]
        if pct_val_2024 > 5:
            ax.text(left_2024 + pct_val_2024/2, 1, f'{pct_val_2024:.1f}%',
                    ha='center', va='center', fontsize=9, fontweight='bold')

        left_2024 += pivot_pct.loc[step, 2024]

    ax.invert_yaxis()
    ax.set_yticks(y_pos)
    ax.set_yticklabels(['2019', '2024'])
    ax.set_xlabel('Percentage of ProductSelection Abandonments')
    ax.set_title(title)
    ax.set_xlim(0, 100)
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=len(step_order), frameon=False)

    fig.tight_layout()
    plt.show()
