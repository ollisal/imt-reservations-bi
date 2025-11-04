"""Visualization for abandonment stage distribution comparison."""

import numpy as np
import matplotlib.pyplot as plt


def plot_abandonment_stages(df):
    """
    Create a stacked horizontal bar chart comparing abandonment stages for 2019 vs 2024.

    Parameters:
    -----------
    df : DataFrame
        Query result with columns: departureyear, finalstage, count
    """
    # Pivot data and calculate percentages
    pivot_df = df.pivot(index='finalstage', columns='departureyear', values='count').fillna(0)
    pivot_pct = pivot_df.div(pivot_df.sum(axis=0), axis=1) * 100

    # Define stage order
    stage_order = ['ProductSelection', 'PassengerInfo', 'ReserverInfo', 'AdditionalServices', 'Confirmation']
    pivot_pct = pivot_pct.reindex(stage_order, fill_value=0)

    # Visualization
    fig, ax = plt.subplots(figsize=(12, 6))

    years = [2019, 2024]
    y_pos = np.arange(len(years))

    # Create stacked horizontal bars
    left_2019 = 0
    left_2024 = 0

    colors = plt.cm.Set3(np.linspace(0, 1, len(pivot_pct.index))) # pyright: ignore[reportAttributeAccessIssue]

    for i, stage in enumerate(pivot_pct.index):
        # 2019 bar (position 0 - top)
        ax.barh(0, pivot_pct.loc[stage, 2019], left=left_2019,
                height=0.5, label=stage, color=colors[i], alpha=0.8)

        # Add percentage text for ProductSelection and PassengerInfo
        if stage in ['ProductSelection', 'PassengerInfo']:
            pct_val = pivot_pct.loc[stage, 2019]
            ax.text(left_2019 + pct_val/2, 0, f'{pct_val:.1f}%',
                    ha='center', va='center', fontsize=9, fontweight='bold')

        left_2019 += pivot_pct.loc[stage, 2019]

        # 2024 bar (position 1 - bottom)
        ax.barh(1, pivot_pct.loc[stage, 2024], left=left_2024,
                height=0.5, color=colors[i], alpha=0.8)

        # Add percentage text for ProductSelection and PassengerInfo
        if stage in ['ProductSelection', 'PassengerInfo']:
            pct_val = pivot_pct.loc[stage, 2024]
            ax.text(left_2024 + pct_val/2, 1, f'{pct_val:.1f}%',
                    ha='center', va='center', fontsize=9, fontweight='bold')

        left_2024 += pivot_pct.loc[stage, 2024]

    ax.invert_yaxis()
    ax.set_yticks(y_pos)
    ax.set_yticklabels(['2019', '2024'])
    ax.set_xlabel('Percentage of Abandoned Reservations')
    ax.set_title('Abandonment Stage Distribution: 2019 vs 2024')
    ax.set_xlim(0, 100)
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=len(stage_order), frameon=False)

    fig.tight_layout()
    plt.show()
