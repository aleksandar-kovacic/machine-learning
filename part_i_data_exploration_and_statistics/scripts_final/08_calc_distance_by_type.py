import os
from dask.distributed import Client
import dask.dataframe as dd
import dask.array as da
import numpy as np
import pandas as pd
import time
from helper_functions import haversine


def read_data() -> dd.DataFrame:
    """
    Lese .parquet Datensatz ein, der vom Skript 02_general_preprocessing generiert wird. Lese nur relevante Spalten ein.
    :return: dask DataFrame
    """
    # einlesen
    filename = "all_TUDA_data.parquet"  # hier Dateinamen ändern
    file_path = os.path.join("..", "..", "data", "TUDA_data", filename)
    df = dd.read_parquet(file_path, columns=["wagon_type", "wagon_ID", "altitude", "latitude", "longitude", "loading_state", "timestamp_measure_position"])
    return df


def organize_data(df: dd.DataFrame) -> dd.DataFrame:
    """
    Entferne NaN Werte, negative Zeitstempel und sorrtiere Daten nach Wagon und Zeitstempel
    :param df: dask DataFrame, welches sortiert und gefiltert werden soll
    :return: aufgeräumtes DataFrame
    """
    # entferne NaN Werte
    df = df.dropna(subset=["wagon_ID", "longitude", "latitude"])

    # entferne NaT
    df = df[df["timestamp_measure_position"] != "NaT"]

    # entferne negative Zeitstempel
    df = df[df["timestamp_measure_position"] >= 0]

    # sortiere DataFrame
    df_sorted = df.map_partitions(
        lambda tmp_df: tmp_df.sort_values(by=["wagon_ID", "timestamp_measure_position"])
    )
    del df
    return df_sorted


def run_processing():
    print('Lese Daten ein...')
    df = read_data()
    print('einlesen erfolgreich! \n')
    print('weiteres preprocessing...')
    df = organize_data(df)
    print('fertig! \n')

    df.compute().to_pickle(os.path.join("..","..","data","output","read_in_data_for_08_calc_distance_by_type.pickle"))    # convert to single csv file
    
    df = pd.read_pickle(os.path.join("..","..","data","output","read_in_data_for_08_calc_distance_by_type.pickle"))

    print('calculating distance...')
    df['distance'] = np.concatenate(df.groupby('wagon_ID').apply(lambda x: haversine(x['latitude'], x['longitude'],
                                                                                     x['latitude'].shift(periods=-1),
                                                                                     x['longitude'].shift(
                                                                                         periods=-1))).values)
    df = df[df['distance'] < 25]

    print('calculation completed! \n')

    print('building dataframe...')

    result = df.groupby(by=["wagon_type", "loading_state"], dropna=False).agg({"distance": "sum"}).unstack()
    print('Berechnung erfolgreich! \n')

    print('erstelle DataFrame...')
    wagon_types = np.unique(result.reset_index().wagon_type)
    df_result = pd.DataFrame({'wagon_type': wagon_types})
    df_dist_empty = result['distance'][0].to_frame(name='distance_empty')
    df_dist_loaded = result['distance'][1].to_frame(name='distance_loaded')
    df_result = df_result.merge(df_dist_empty, how='left', on='wagon_type')
    df_result = df_result.merge(df_dist_loaded, how='left', on='wagon_type')

    df_result = df_result.fillna(0)
    df_result['distance_total'] = df_result['distance_empty'] + df_result['distance_loaded']

    print('done! \n')

    print('saving dataframe...')
    if os.path.exists(os.path.join("..", "output", "group_02")) == False:
        os.mkdir(os.path.join("..", "output", "group_02"))
    file_path = os.path.join("..", "output", "group_02","distance_all.csv")
    df_result.to_csv(file_path, index=False)
    print('done! \n')


if __name__ == '__main__':
    start = time.time()
    client = Client(n_workers=6, threads_per_worker=2)
    run_processing()
    print(time.time() - start)
    client.shutdown()
