import io
from itertools import cycle

import seaborn as sns
import matplotlib.pyplot as plt

xtick_names = {
    'DistributionBased': 'DB',
    'JaccardLevenMatcher': 'JL',
    'Cupid': 'CU',
    'SimilarityFlooding': 'SF',
    'SemProp': 'SP',
    'COMA-SI': 'COI',
    'COMA-S': 'COS',
    'EmbDI': 'EDI'
}

medianprops = dict(linewidth=3.5)
current_palette = sns.color_palette('colorblind')


def plot_by_data_type(dataframe, data_index, xlabel_order, colors, pattern, plot_name, hue_order, cpmap):
    cols = 4
    fig, axe = plt.subplots(1, cols, figsize=(30, 5), sharey=True)
    plt.rcParams["legend.facecolor"] = 'white'
    sns.set(style="whitegrid", font_scale=2)

    ylabel = 'Recall at size of\nground truth'

    instance_handles = list()
    instance_labels = list()

    # instance algorithms and plots
    for i, p in enumerate(dataframe.keys()):

        dd = dataframe[p][data_index]
        if data_index == 1:  # we don't display the verbatim schemata for schema based because all values eq 1
            dd = dd.drop(dd[dd['SplitType'] == 'Verbatim Schemata'].index)  # No verbatim schemata

        # make the bars same size for any case
        if len(dd['Algorithms'].unique()) == 2:
            inst_bars = sns.boxplot(x="Algorithms", y="value", data=dd, saturation=.5, width=0.55,
                                    hue='SplitType', hue_order=hue_order, palette=colors, ax=axe[i],
                                    medianprops=medianprops,
                                    order=xlabel_order)
        elif len(dd['SplitType'].unique()) > 1:
            inst_bars = sns.boxplot(x="Algorithms", y="value", data=dd, saturation=.5,
                                    hue='SplitType', hue_order=hue_order, palette=colors, ax=axe[i],
                                    medianprops=medianprops,
                                    order=xlabel_order)
        elif any('Noisy Schemata' in a for a in dd['SplitType'].unique()):
            inst_bars = sns.boxplot(x="Algorithms", y="value", data=dd, saturation=.5, width=0.4,
                                    hue='SplitType', hue_order=hue_order, palette=[colors[1]], ax=axe[i],
                                    medianprops=medianprops,
                                    order=xlabel_order)
        elif any('Noisy Instances' in a for a in dd['SplitType'].unique()):
            inst_bars = sns.boxplot(x="Algorithms", y="value", data=dd, saturation=.5,
                                    hue='SplitType', hue_order=hue_order, palette=[colors[0]], ax=axe[i],
                                    medianprops=medianprops,
                                    order=xlabel_order)
        else:
            inst_bars = sns.boxplot(x="Algorithms", y="value", data=dd, saturation=.5,
                                    hue='SplitType', hue_order=hue_order, palette=[colors[1]], ax=axe[i],
                                    medianprops=medianprops,
                                    order=xlabel_order)

        axe[i].set_title(p, fontsize=25, pad=20)

        # find the plots that have the maximum columns to show it in the legend
        h, l = axe[i].get_legend_handles_labels()
        if len(h) > len(instance_handles):
            instance_handles = h
            instance_labels = l

        # add the corresponding patterns to the legend
        len_bars = len(inst_bars.patches)
        for i, bar in enumerate(inst_bars.patches):
            if i < len_bars / 2:
                bar.set_hatch(pattern[0])
            else:
                bar.set_hatch(pattern[1])

        # add patterns to the boxes
        cycle_pattern = cycle(pattern)
        for i, bar in enumerate(inst_bars.artists):
            c = bar.get_facecolor()
            hatch = list(filter(lambda e: e[0] - c[0] < 0.1, cpmap.keys()))[0]
            bar.set_hatch(cpmap[hatch])

    # if all plots have only one type of bars, show the last one in the legend
    if instance_handles is None:
        instance_handles = h
    if instance_labels is None:
        instance_labels = l

    for i in range(0, cols):
        # add the ticklabels for the algorithms
        ticklabels = [xtick_names[l.get_text()] for l in axe[i].get_xticklabels()]
        axe[i].set_xticklabels(ticklabels)

        # remove the ylabel from everywhere except the first column
        # remove all the legends
        axe[i].set_ylabel('')
        axe[i].set_xlabel('')
        axe[i].get_legend().remove()

    axe[0].set_ylabel(ylabel, fontsize=25)

    # hack to add the legend
    if data_index == 0:
        axe[1].legend(instance_handles, instance_labels, bbox_to_anchor=(0.3, -0.4), loc=4,
                      borderaxespad=0., ncol=2)
        plt.plot([], [], ' ', label="DB - DistributionBased")
        plt.plot([], [], ' ', label="JL - JaccardLevenMatcher")
        plt.plot([], [], ' ', label="COI - COMA-Instance")

        hh, ll = plt.gca().get_legend_handles_labels()
        legend = plt.legend(hh[0:-2], ll[0:-2], bbox_to_anchor=(0.5, -0.4), loc=4, borderaxespad=0., ncol=6,
                            handlelength=0.1, handletextpad=0.1)
    elif data_index == 1:
        axe[1].legend(instance_handles, instance_labels, bbox_to_anchor=(-0.4, -0.4), loc=4,
                      borderaxespad=0., ncol=2)
        plt.plot([], [], ' ', label="CU - Cupid")
        plt.plot([], [], ' ', label="SF - SimilarityFlooding")
        plt.plot([], [], ' ', label="COS - COMA-Schema")

        hh, ll = plt.gca().get_legend_handles_labels()
        legend = plt.legend(hh[0:-1], ll[0:-1], bbox_to_anchor=(-0.6, -0.4), loc=4, borderaxespad=0., ncol=6,
                            handlelength=0.1, handletextpad=0.1)
    else:
        axe[1].legend(instance_handles, instance_labels, bbox_to_anchor=(0.8, -0.4), loc=4,
                      borderaxespad=0., ncol=4)
        plt.plot([], [], ' ', label="EDI - EmbDI")
        plt.plot([], [], ' ', label="SP - SemProp")

        hh, ll = plt.gca().get_legend_handles_labels()
        legend = plt.legend(hh[0:-2], ll[0:-2], bbox_to_anchor=(-0.2, -0.4), loc=4, borderaxespad=0., ncol=6,
                            handlelength=0.1, handletextpad=0.1)

    fig.subplots_adjust(wspace=0.01, hspace=0.25)

    buffer = io.BytesIO()
    # plt.savefig('plots/{}.png'.format(plot_name), dpi=300, bbox_inches="tight")
    plt.savefig(buffer, format="png", dpi=300, bbox_inches="tight")
    plt.close()
    return buffer




