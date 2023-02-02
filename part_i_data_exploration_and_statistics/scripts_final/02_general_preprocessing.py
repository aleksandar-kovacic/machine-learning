# imports
import os
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
from helper_functions import format_timestamp
import time


def run_preprocessing():
    # Lese Daten ein
    print('Daten einlesen...')
    filepath = os.path.join("..", "..", "data", "TUDA_data", "*_data.csv")
    ddf = dd.read_csv(filepath)
    print('... fertig!\n')

    print('DataFrame aufräumen...')
    ddf = ddf.dropna(subset=['wagon_ID', 'latitude', 'longitude'])
    ddf = ddf[ddf.provider != 0]

    ddf = ddf.replace({'wagon_ID': '\'', 'loading_state': 'Beladen'},
                      {'wagon_ID': '', 'loading_state': '1'}, regex=True)
    ddf = ddf.replace({'loading_state': 'Leer'},
                      {'loading_state': '0'}, regex=True)

    # strings zu int
    ddf.wagon_ID = ddf.wagon_ID.astype('int64')     #int64 für Windows, da int32 zu kurz für die ID ist
    ddf.loading_state = ddf.loading_state.astype('int')

    # ignoriere wagon IDs (siehe FAQs)
    ddf = ddf[ddf.wagon_ID != 1209603222683999]
    ddf = ddf[ddf.wagon_ID != 1176603735805403]
    print('... fertig!\n')

    print('Stelle Zeit in Sekunden dar...')
    ddf.timestamp_measure_position = ddf.timestamp_measure_position.map(format_timestamp, meta=('total_time_in_sec', float))
    ddf.timestamp_transfer = ddf.timestamp_transfer.map(format_timestamp, meta=('total_time_in_sec', float))
    ddf.timestamp_measure_movement_state = ddf.timestamp_measure_movement_state.map(
        lambda x: format_timestamp(x) if x != 'NaT' else float('NaN'), meta=('total_time_in_sec', float)) 
    ddf.timestamp_index = ddf.timestamp_index.map(format_timestamp, meta=('total_time_in_sec', float))
    print('... done!\n')

    # Kodiere movement_state als Integer: moving=2, standing=1, parking=0
    ddf = ddf.replace({'movement_state': 'moving'}, {'movement_state': '2'}, regex=True)
    ddf = ddf.replace({'movement_state': 'standing'}, {'movement_state': '1'}, regex=True)
    ddf = ddf.replace({'movement_state': 'parking'}, {'movement_state': '0'}, regex=True)
    ddf.movement_state = ddf.movement_state.astype(float)

    print('Lade mapping DataFrame...')
    # Überspringe jede Zeile, in der eine Wagon ID nicht im mapping DataFrame ist
    map_path_pickle = os.path.join("..", "..", "data", "mappingDf.pickle")
    mapping_df = pd.read_pickle(map_path_pickle)
    mapping_ddf = dd.from_pandas(mapping_df, npartitions=6)
    print('... fertig!\n')

    print('Füge wagon_type und wagon_construction zum DataFrame hinzu...')
    mapping_ddf = mapping_ddf.dropna()
    mapping_ddf = mapping_ddf.drop('provider', axis=1)
    ddf = dd.merge(ddf, mapping_ddf, on='wagon_ID')
    print('... fertig!\n')

    print('speichere DataFrame im parquet Format...')
    # wird als parquet Datei gespeichert für die weitere Verwendung
    path_to_parquet = os.path.join("..", "..", "data", "TUDA_data", "all_TUDA_data.parquet")
    ddf.to_parquet(path_to_parquet)
    print('... fertig!\n')


if __name__ == '__main__':
    start = time.time()
    # hier Anzahl der Kerne und Threads pro Kern einstellen!
    client = Client(n_workers=8, threads_per_worker=2)
    run_preprocessing()
    print('Berechnung hat %f Sekunden gedauert.' % (time.time() - start))
    client.shutdown()
