from dask.distributed import Client
import dask.dataframe as dd
import dask.array as da
import numpy as np
import time
import os
import reverse_geocoder as rg
import country_converter as coco
import pandas as pd
from helper_functions import get_distance, haversine


def read_data() -> dd.DataFrame:
    """
    Lese .parquet Datensatz ein, der vom Skript 02_general_preprocessing generiert wird. Lese nur relevante Spalten ein.
    :return: dask DataFrame
    """
    # read
    filename = "all_TUDA_data.parquet"  # insert filename here
    file_path = os.path.join("..", "..", "data", "TUDA_data", filename)
    df = dd.read_parquet(file_path,
                         columns=['wagon_ID', 'loading_state', 'latitude', 'longitude', 'timestamp_measure_position']).reset_index()
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
    df = df.map_partitions(
        lambda tmp_df: tmp_df.sort_values(by=["wagon_ID", "timestamp_measure_position"])
    )
    return df


def get_country_geocoder(coordinates: np.ndarray):
    """
    Nutze die reverse_geocoder Bibliothek um von den Koordinaten auf die Länder zu schließen
    :param coordinates: Liste von Tuplen, welche jeweils Koordinaten beinhalten (latitude, longitude)
    :return: Liste von Strings mit den dazugehörigen Länder Codes
    """
    results = rg.search(coordinates)
    country_codes = list(map(lambda d: d['cc'], results))
    return country_codes


def add_country_column(df: dd.DataFrame) -> dd.DataFrame:
    """
    Fügt die Spalte "country" zum DataFrame hinzu
    """
    vec_lat = df['latitude']
    vec_lon = df['longitude']
    chunks_in_df = tuple(df.map_partitions(len))
    df['country'] = da.from_array(get_country_geocoder(list(zip(vec_lat, vec_lon))), chunks=chunks_in_df)
    return df


def run_processing():
    print('Daten einlesen...')
    df = read_data()
    print('fertig! \n')
    print('weiteres preprocessing...')
    df = organize_data(df)
    print('fertig! \n')
    print('Füge "country" Spalte hinzu...')
    df = add_country_column(df)
    print('fertig! \n')


    df.to_parquet(os.path.join("..","..","data","output","preproc_calc_distance_by_country.parquet"))  # save as parquet
    df = dd.read_parquet(os.path.join("..","..","data","output","preproc_calc_distance_by_country.parquet"))    
    df.compute().to_csv(os.path.join("..","..","data","output","preproc_calc_distance_by_country.csv"), index=False)    # convert to single csv file

    df = pd.read_csv(os.path.join("..","..","data","output","preproc_calc_distance_by_country.csv"))

    print('calculating distance...')
    df['distance'] = np.concatenate(df.groupby('wagon_ID').apply(lambda x: haversine(x['latitude'], x['longitude'],
                                                                                     x['latitude'].shift(periods=-1),
                                                                                     x['longitude'].shift(
                                                                                         periods=-1))).values)
    df = df[df['distance'] < 25]

    print('calculation completed! \n')

    print('Berechne gefahrene Distanz pro Land und loading_state...')
    result = df.groupby(by=["country", "loading_state"], dropna=False).agg({"distance": "sum"}).unstack()
    print('Berechnung erfolgreich! \n')

    print('erstelle DataFrame...')
    countries = np.unique(result.reset_index().country)
    df_result = pd.DataFrame({'country': countries})
    df_dist_empty = result['distance'][0].to_frame(name='distance_empty')
    df_dist_loaded = result['distance'][1].to_frame(name='distance_loaded')
    df_result = df_result.merge(df_dist_empty, how='left', on='country')
    df_result = df_result.merge(df_dist_loaded, how='left', on='country')

    df_result = df_result.fillna(0)
    df_result['distance_total'] = df_result['distance_empty'] + df_result['distance_loaded']

    print('done! \n')

    print('Konvertiere Länder Codes zu Ländernamen...')
    df_result['country'] = df_result.country.apply(lambda country: coco.convert(names=country, to='name_short'))
    print('fertig! \n')

    print('speichere DataFrame...')
    if os.path.exists(os.path.join("..", "output", "group_02")) == False:
        os.mkdir(os.path.join("..", "output", "group_02"))
    file_path = os.path.join("..", "output", "group_02","distance_countries.csv")
    df_result.to_csv(file_path)
    print('fertig! \n')


if __name__ == '__main__':
    start = time.time()
    client = Client(n_workers=6, threads_per_worker=2)
    run_processing()
    print(time.time() - start)
    client.shutdown()