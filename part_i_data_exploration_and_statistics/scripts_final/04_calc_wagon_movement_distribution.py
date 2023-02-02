from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
import numpy as np
import time
import os
import glob


def read_data(ds) -> dd.DataFrame:
    """
    reads data from all_TUDA_data.parquet file that is generated in 04_05_dask_example_01_preprocess.py
    :return: cleaned dataframe
    """
    # read
    file = os.path.join(ds)
    pdf = pd.read_pickle(file)
    df = dd.from_pandas(pdf, npartitions=6)

    # drop negative timestamps!
    df = df[df.timestamp_measure_movement_state >= 0]
    return df


def organize_data(df: dd.DataFrame) -> dd.DataFrame:
    """
    drop data that is not needed and sort data by wagon ID and timestamp
    :param df: dask dataframe to organize
    :return: sorted and filtered dask dataframe
    """
    # sort dataframe
    df_time_calc = df.map_partitions(
        lambda tmp_df: tmp_df.sort_values(by=["wagon_ID", "timestamp_measure_movement_state"])
    )
    del df
    return df_time_calc


def calc_duration(df_time_calc: dd.DataFrame) -> dd.DataFrame:
    """
    calculates the duration of a movement operation by looking at the
    differences of the timestamp between to different movement states,
    adds column diff_timestamp to dataframe
    """
    # calculate difference of timestamp to get duration of a movement state
    df_time_calc['diff_timestamp'] = df_time_calc['timestamp_measure_movement_state'].diff(periods=-1)
    condition = (df_time_calc.wagon_ID.shift(periods=-1) != df_time_calc.wagon_ID)
    df_time_calc.diff_timestamp = df_time_calc.diff_timestamp.mask(condition, 0)
    df_time_calc.diff_timestamp = df_time_calc.diff_timestamp.abs()
    return df_time_calc


def calc_week_and_day(df_time_calc: dd.DataFrame):
    """
    adds column for time in days and time in weeks to dataframe and calculate time in days
    :param df_time_calc: dask dataframe
    :return df_time_calc: dask dataframe with added columns
    :return bins_second_labels: list containing time in days
    """
    # Daten in Zeitraum von 24h formatieren
    day_seconds = 24*60*60

    if min(df_time_calc["timestamp_measure_movement_state"]) > 0:
        startSec = int( np.arange(0, min(df_time_calc["timestamp_measure_movement_state"]), day_seconds)[-1] )
    else:
        startSec = 0
    skipDays = int(startSec/day_seconds)     #überspringen die Tage in der Benennung. Bspw, wenn datenset erst ab Tag 9 anfängt, werden die Tage 1-8 nicht betrachtet, aber Benennung beginnt von 9 an, für den globalen Bezug.

    bins_seconds = np.arange( startSec, max(df_time_calc["timestamp_measure_movement_state"])+day_seconds, day_seconds)  # Einteilung der Sekunden in Tage von t_start bis t_max, Schrittweite = 1 Tag
    bins_seconds = bins_seconds.tolist()   #array->list

    bins_seconds_labels = np.arange(1+skipDays, len(bins_seconds)+skipDays, 1).tolist()   # bezeichnung / index für die bereiche, day 1,2,3,... Day index beginnt ab 1!
    
    # add column for bin (In welchem Zeitbereich bzw. Tag sich die jeweilige Messung befindet.)
    df_time_calc['day_measure_movement_state'] = df_time_calc.timestamp_measure_movement_state.map_partitions(
        pd.cut, bins=bins_seconds, labels=bins_seconds_labels
    )

    return df_time_calc, bins_seconds_labels


def preproc_ddf_to_csv():
    ddf = dd.read_parquet(os.path.join("..","..","data","output","preproc_TUDA_data_for_wgMovementCalc.parquet"))
    ddf = ddf.compute()
    result_path = os.path.join("..", "..", "data", "output", "preproc_TUDA_data_for_wgMovementCalc.csv")
    ddf.to_csv(result_path, index=False)

def preproc_add_diff_timestamp():
    file_path = os.path.join("..", "..", "data", "TUDA_data", "all_TUDA_data.parquet")
    ddf = dd.read_parquet(file_path,
                        columns=["wagon_ID", "movement_state", "wagon_type", "timestamp_measure_movement_state"])
    ddf = ddf.dropna(subset=["movement_state"])
    ddf['movement_state'] = ddf['movement_state'].astype('int8')
    ddf['wagon_type'] = ddf['wagon_type'].astype('int8')
    ddf = organize_data(ddf)
    ddf = calc_duration(ddf)
    ddf, _ = calc_week_and_day(ddf)
    ddf = ddf[ddf['diff_timestamp'] <= 86400]
    ddf = ddf[["wagon_ID","movement_state","wagon_type","diff_timestamp", "day_measure_movement_state"]]
    ddf = ddf.map_partitions( lambda tmp_df: tmp_df.sort_values(by=["wagon_ID","day_measure_movement_state","movement_state"]) )
    
    ddf.to_parquet(os.path.join("..","..","data","output","preproc_TUDA_data_for_wgMovementCalc.parquet"))  # save as parquet
    del ddf
    preproc_ddf_to_csv()


def process_allWgMovingTime_totalMovingTime():
    for mv_state in range(3):
        file_path = os.path.join("..", "..", "data", "TUDA_data", "all_TUDA_data.parquet")
        ddf = dd.read_parquet(file_path,
                            columns=["wagon_ID", "movement_state", "wagon_type", "timestamp_measure_movement_state"])
        ddf = ddf.dropna(subset=["movement_state"])
        ddf['movement_state'] = ddf['movement_state'].astype('int8')
        ddf['wagon_type'] = ddf['wagon_type'].astype('int8')
        ddf = organize_data(ddf)
        ddf = calc_duration(ddf)
        ddf = ddf[ddf['diff_timestamp'] <= 86400]
        ddf = ddf[["wagon_ID","movement_state","wagon_type","diff_timestamp"]]

        ddf = ddf[ddf.movement_state == mv_state]

        grouper = ddf.groupby(["wagon_type", "wagon_ID"])
        ddf = grouper['diff_timestamp'].sum().to_frame(name = 'total_movingTime').reset_index()
        ddf = ddf[(ddf.total_movingTime != 0)]

        df = ddf.compute()

        result_path = os.path.join("..", "..", "data", "output", "all_wgID_total_moventTime_mvState="+str(int(mv_state))+".csv")
        df.to_csv(result_path, index=False)


def process_allWgMovingTime_dailyMeanMovingTime():
    for mv_state in range(3):
        df = pd.read_csv(os.path.join("..", "..", "data", "output", "preproc_TUDA_data_for_wgMovementCalc.csv"))
        df = df[df.movement_state == mv_state]

        grouper = df.groupby(["wagon_type", "wagon_ID", "day_measure_movement_state"])
        df = grouper['diff_timestamp'].sum().to_frame(name = 'movingTimePerDay').reset_index()
        df = df[(df.movingTimePerDay != 0)]

        grouper = df.groupby(["wagon_type", "wagon_ID"])
        df = grouper['movingTimePerDay'].mean().to_frame(name = 'mean_movingTimePerDay').reset_index()
        df = df[(df.mean_movingTimePerDay != 0)]

        result_path = os.path.join("..", "..", "data", "output", "all_wgID_dailyMean_moventTime_mvState="+str(int(mv_state))+".csv")
        df.to_csv(result_path, index=False)


if __name__ == '__main__':
    start = time.time()
    # set number of cpu cores and number of threads per core here
    client = Client(n_workers=8, threads_per_worker=2)
    
    preproc_add_diff_timestamp()  ##only execute at the first run
    process_allWgMovingTime_totalMovingTime()
    process_allWgMovingTime_dailyMeanMovingTime()
    print('calculation took %f seconds.' % (time.time() - start))
    client.shutdown()