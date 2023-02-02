# imports
import pandas as pd
import dask.dataframe as dd
import dask.array as da
from dask.distributed import Client

import numpy as np
from matplotlib import pyplot as plt
import seaborn as sn

from os.path import join
from time import time


def gen_bxp_provider(df: dd.DataFrame): 
    """
    Erstellt Boxplots für jedes Feature in Abhängigkeit des Providers
    :param df: Pandas DataFrame mit Spalten [feature, provider]
    """
    df = df.compute()
    labels = {"latency": "Latenz", "GNSS_quality": "GPS Qualität", "mobile_quality": "Mobilfunk Qualität"}
    this_label = labels[df.columns[0]]
    plt.style.use(['mystyle'])
    fig = plt.figure()
    ax = sn.boxplot(x= "provider", y=df.columns[0], width=0.5, data=df, showfliers=False)
    ax.set(xlabel='Provider', ylabel=this_label)
    plt.title(this_label)
    fig.savefig(join('..', 'output', df.columns[0] + '_provider.pdf'))
    plt.close(fig)
    del df


def gen_plots():
    # parquet Datei einlesen
    parquet_path = join('..', 'output', 'part2_features.parquet')
    df_features = dd.read_parquet(parquet_path)

    # matplotlib konfigurieren
    plt.style.use(['mystyle'])
    
    # Berechnungen
    print('Berechne Korrelationsmatrix')
    # Korrelation
    corrMatrix = df_features[['mobile_quality', 'GNSS_quality', 'latency']].corr().compute() 
    print('Berechne relative Standardabweichung')
    # relative Standardabweichung
    std_rel = (df_features[['mobile_quality', 'GNSS_quality', 'latency']].std().compute()
            / df_features[['mobile_quality', 'GNSS_quality', 'latency']].mean().compute()).to_numpy() 
    print('Berechne Standardfehler')
    # Standardfehler
    std_errs = []
    std_errs_rel = []
    for feature in df_features[['mobile_quality', 'GNSS_quality', 'latency']].columns:
        clean_feature = df_features[feature].dropna()
        std_err = clean_feature.std() / len(clean_feature)
        std_err = std_err.compute()
        std_errs.append(std_err)
        std_errs_rel.append(std_err/clean_feature.mean().compute())

    print('Erstelle plots') 
    ### plots
    labels_features = ['Qualität Mobilfunk', 'Qualität GPS', 'Latenz'] 
    output_path = join('..', 'output')

    # Korrelation Heatmap
    fig = plt.figure(figsize=(7, 7))
    sn.heatmap(corrMatrix, annot=True, cmap='RdYlBu', xticklabels=labels_features, yticklabels=labels_features)
    plt.title('Korrelation der Features')
    fig.savefig(join(output_path, 'features_correlation.pdf'))
    
    # Standardabweichung
    fig = plt.figure(figsize=(10, 3))
    ax = fig.add_axes([0,0,1,1])
    ax.bar(labels_features, std_rel)
    plt.grid(False)
    plt.title(r'relative Standardabweichung $\sigma_{rel}$')
    fig.savefig(join(output_path, 'features_std.pdf'))

    # Standardfehler 
    fig, axs = plt.subplots(1, 3, figsize=(10, 3))
    for ax, label, err in zip(axs, labels_features, std_errs):
        ax.bar(label, err)
        ax.grid(False)
    fig.suptitle('Standardfehler')
    fig.savefig(join(output_path, 'features_std_error.pdf'))

    fig = plt.figure(figsize=(10, 3))
    ax = fig.add_axes([0,0,1,1])
    ax.bar(labels_features, std_errs_rel)
    plt.title('relativer Standardfehler')
    plt.grid(False)
    fig.savefig(join(output_path, 'features_std_error_rel.pdf'))
    
    # boxplots
    print('Erstelle Boxplot')
    for col in ['mobile_quality', 'GNSS_quality', 'latency']:
        print('Erstelle Boxplot pro Provider für feature %s.' %(col))
        gen_bxp_provider(df_features[[col, 'provider']])

    print('Erstelle Subplot...')
    fig, axs = plt.subplots(1, 3, figsize=(10, 3))
    fig.suptitle('Boxplots der features')
    for idx, col in enumerate(['mobile_quality', 'GNSS_quality', 'latency']):
        print(col)
        this_feat = df_features[col].compute().to_frame()
        sn.boxplot(y=col, data=this_feat, ax=axs[idx], showfliers=False)
        del this_feat
        axs[idx].set_xlabel(labels_features[idx])
    fig.savefig(join(output_path, 'boxplots_features.pdf'))
    print('fertig!')
    

if __name__ == '__main__':
    # dask Client initialisieren
    print('Initialisiere Client')
    client = Client(n_workers=8, threads_per_worker=2)
    print('fertig')
    t_start = time()
    gen_plots()
    print('Berechnung in %f Sekunden beendet.' %(time() - t_start))

