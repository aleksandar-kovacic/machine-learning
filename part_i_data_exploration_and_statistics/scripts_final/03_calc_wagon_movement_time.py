#imports:
from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
import numpy as np
import time
import os
import glob
from helper_functions import format_timestamp


# Funktion, um Benennung von Dateien abhängig vom index anzupassen. Z.B. file01, file02, ..., file10, file11, ...
# schafft Platzhalter in Form von "0" oder ""
def zeroNumbersCalc(it):
    if it < 10:
        zeroNumbers = "0"
    else:
        zeroNumbers = ""
    return zeroNumbers


def preProcCsvToPickle():
    """ Dieses Preprocessing liest die csv Rohdaten ein, filtert die für diese Betrachtung relevanten Spalten 
    ["wagon_ID", "movement_state", "wagon_type", "timestamp_measure_movement_state"], bereinigt und formatiert die Daten.
    Dann abspeichern der einzelnen Daten als pickel Datei. Das Preprocessing muss nur einmalig ausgeführt werden! """
    print("running preproc for csv files...")
    readList = np.arange(1, 45+1, 1).tolist()

    for ii in readList:
        print("pre-processing dataset %s%i" %(zeroNumbersCalc(ii), ii))
        file = os.path.join("..", "..", "data", "TUDA_data", zeroNumbersCalc(ii)+str(ii)+"_*_TUDA_data.csv")
        df = dd.read_csv(file)
        #preproc
        df = df[["wagon_ID", "movement_state", "timestamp_measure_movement_state"]]
        df = df.dropna()
        df = df.replace({'wagon_ID': '\''}, {'wagon_ID': ''}, regex=True)
        df = df.replace({'movement_state': 'moving'}, {'movement_state': '2'}, regex=True)
        df = df.replace({'movement_state': 'standing'}, {'movement_state': '1'}, regex=True)
        df = df.replace({'movement_state': 'parking'}, {'movement_state': '0'}, regex=True)
        df.movement_state = df.movement_state.astype(float)

        df.wagon_ID = df.wagon_ID.astype('int64')
        df = df[df.timestamp_measure_movement_state != 'NaT']
        df.timestamp_measure_movement_state = df.timestamp_measure_movement_state.map(
            format_timestamp, meta=('total_time_in_sec', float))
        # if wagon_ID not in mapping file, remove row
        map_path_pickle = os.path.join("..", "..", "data", "mappingDf.pickle")
        mapping_df = pd.read_pickle(map_path_pickle)
        mapping_ddf = dd.from_pandas(mapping_df, npartitions=6)
        mapping_ddf = mapping_ddf.dropna()
        mapping_ddf = mapping_ddf.drop('provider', axis=1)
        df = dd.merge(df, mapping_ddf, on='wagon_ID')
        # filter required data
        df = df[["wagon_ID", "movement_state", "wagon_type", "timestamp_measure_movement_state"]]

        #data to pickle:
        if os.path.exists(os.path.join("..", "..", "data", "TUDA_data", "pickleDataGroup1")) == False:
            os.mkdir(os.path.join("..", "..", "data", "TUDA_data", "pickleDataGroup1"))
        pathToPickle = os.path.join("..", "..", "data", "TUDA_data", "pickleDataGroup1", zeroNumbersCalc(ii)+str(ii) + ".pickle")
        df = df.compute()
        df.to_pickle(pathToPickle)


def preProcDatasetsToDaysFiles():
    """Dieses Preprocessing liest die pickel Dateien ein, fügt abhängig von timestamp_measure_movement_state die Spalte day hinzu.
    Dann werden die Daten in dayfiles nach Tagen 1-10, 11-20, etc. abgespeichert. """

    print("running preproc for preprocessed dataset files into daychunk files...")
    readList = np.arange(1, 45+1, 1).tolist()
    for ds in readList:
        print("processing dataset %s%i" %(zeroNumbersCalc(ds), ds))
        file = os.path.join("..", "..", "data", "TUDA_data", "pickleDataGroup1", zeroNumbersCalc(ds)+str(ds)+".pickle")
        pdf = pd.read_pickle(file)
        df = dd.from_pandas(pdf, npartitions=6)
        # drop negative timestamps!
        df = df[df.timestamp_measure_movement_state >= 0]
        # Daten in Zeitraum von 24h formatieren
        day_seconds = 24*60*60
        if min(df["timestamp_measure_movement_state"]) > 0:
            startSec = int( np.arange(0, min(df["timestamp_measure_movement_state"]), day_seconds)[-1] )
        else:
            startSec = 0
        skipDays = int(startSec/day_seconds)     #überspringen die Tage in der Benennung. Bspw, wenn datenset erst ab Tag 9 anfängt, werden die Tage 1-8 nicht betrachtet, aber Benennung beginnt von 9 an, für den globalen Bezug.
        bins_seconds = np.arange( startSec, max(df["timestamp_measure_movement_state"])+day_seconds, day_seconds)  # Einteilung der Sekunden in Tage von t_start bis t_max, Schrittweite = 1 Tag
        bins_seconds = bins_seconds.tolist()   #array->list
        bins_seconds_labels = np.arange(1+skipDays, len(bins_seconds)+skipDays, 1).tolist()   # bezeichnung / index für die bereiche, day 1,2,3,... Day index beginnt ab 1!
        # add column for bin (In welchem Zeitbereich bzw. Tag sich die jeweilige Messung befindet.)
        df['day_measure_movement_state'] = df.timestamp_measure_movement_state.map_partitions(
            pd.cut, bins=bins_seconds, labels=bins_seconds_labels
        )
        #speichere Daten von day 1-10, 11-20,.. in zugehörige Dateien:
        if os.path.exists(os.path.join("..", "..", "data", "TUDA_data", "pickleDataGroup1", "dayFiles")) == False:
            os.mkdir(os.path.join("..", "..", "data", "TUDA_data", "pickleDataGroup1", "dayFiles"))
        dsStartDay = bins_seconds_labels[0]
        dsEndDay = bins_seconds_labels[-1]
        #chunk days into 10 steps:
        dayChunks = np.arange(dsStartDay//10*10, dsEndDay+10, 10).tolist()
        for i in range(len(dayChunks)-1):
            #jeweils von dayChunks[i]+1 bis dayChunks[i+1], e.g. day 1-10, 11-20, ...
            startDay, endDay = dayChunks[i]+1 , dayChunks[i+1]
            startDayNaming, endDayNaming = startDay, endDay
            if endDay > dsEndDay:
                endDay = dsEndDay
            if startDay < dsStartDay:
                startDay = dsStartDay
            tempDf = df[ (df.day_measure_movement_state >= startDay) & (df.day_measure_movement_state <= endDay) ]
            tempDf = tempDf.compute()
            #check if file already exists, if yes: append the data, if no: create new file and put that data in:
            file = str(startDayNaming) + "-" + str(endDayNaming) + ".pickle"
            filepath = os.path.join("..", "..", "data", "TUDA_data", "pickleDataGroup1", "dayFiles", file)
            if os.path.exists(filepath) == False:
                tempDf.to_pickle(filepath)
            else:
                existDf = pd.read_pickle(filepath)
                tempDf = pd.concat([existDf, tempDf], axis=0, ignore_index=True)
                tempDf.to_pickle(filepath)
                del existDf
            del tempDf
        del df
            



def read_data(ds) -> dd.DataFrame:
    """    Einlesen der gepickelten day-files. Konvertierung in dask dataframe. """
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


def wgTypeMovingTime(wgType: int, df_time_calc: dd.DataFrame, days: list):
    """
    Für wagon_type 'wgType' wertet diese Funktion die Werte mean, median, std und error=standard_deviation/sqrt(n)
    für die moving/standing/parking Time über den timestamps in Tagen aus.
    Es werden die statistischen Parameter für jeden Wagon eines Wagon Types für jeden day berechnet.
    :param wgType: integer describing wagon type
    :param df_time_calc: dask DataFrame containing data
    :param days: days to evaluate data
    :return: pandas DataFrame containing all relevant information of moving time
    """
    # extract information for one wagon type
    df_wgType = df_time_calc[df_time_calc["wagon_type"] == wgType]

    # statistical parameters: mean, median, std=standard_deviation, error=std/sqrt(n), -> at the index 0,1,2,3
    movingTimePerDayStats = [[], [], [], []]
    standingTimePerDayStats = [[], [], [], []]
    parkingTimePerDayStats = [[], [], [], []]

    # group dataframe and calculate sum of moving time for each group.
    # data is computed so movingTimeSumsSeries is a pandas Series object
    movingTimeSumsSeries = df_wgType.groupby(
        ["day_measure_movement_state", "wagon_ID", "movement_state"]).diff_timestamp.sum().compute()

    for day in days:
        # fill lists for the states: parking, standing and moving
        for movement_state, stat_list in enumerate(
                [parkingTimePerDayStats, standingTimePerDayStats, movingTimePerDayStats]):
            try:
                stat_list[0].append(movingTimeSumsSeries.xs(day, level='day_measure_movement_state').xs(
                    movement_state, level='movement_state').mean())
                stat_list[1].append(movingTimeSumsSeries.xs(day, level='day_measure_movement_state').xs(
                    movement_state, level='movement_state').median())
                stat_list[2].append(movingTimeSumsSeries.xs(day, level='day_measure_movement_state').xs(
                    movement_state, level='movement_state').std())
                stat_list[3].append(movingTimeSumsSeries.xs(day, level='day_measure_movement_state').xs(
                    movement_state, level='movement_state').std() / np.sqrt(len(
                    movingTimeSumsSeries.xs(day, level='day_measure_movement_state').xs(2, level='movement_state')
                )))
            except:
                print('no data fround for moving state ' + str(movement_state) + ' and day ' + str(day))

    # after iterating over all days for this wagon type, return the data in a pandas dataframe:
    df_wgTypeMovingTime = pd.DataFrame({"wagon_type": np.ones(len(movingTimePerDayStats[0])) * wgType,
                                        "day": days,
                                        "moving_time_mean": movingTimePerDayStats[0],
                                        "moving_time_median": movingTimePerDayStats[1],
                                        "moving_time_std": movingTimePerDayStats[2],
                                        "moving_time_error": movingTimePerDayStats[3],
                                        "standing_time_mean": standingTimePerDayStats[0],
                                        "standing_time_median": standingTimePerDayStats[1],
                                        "standing_time_std": standingTimePerDayStats[2],
                                        "standing_time_error": standingTimePerDayStats[3],
                                        "parking_time_mean": parkingTimePerDayStats[0],
                                        "parking_time_median": parkingTimePerDayStats[1],
                                        "parking_time_std": parkingTimePerDayStats[2],
                                        "parking_time_error": parkingTimePerDayStats[3]})                              
    return df_wgTypeMovingTime


def run_processing():
    #create the output folder, if it doesnt exists
    if os.path.exists(os.path.join("..", "..", "data", "output")) == False:
        os.mkdir(os.path.join("..", "..", "data", "output"))
    if os.path.exists(os.path.join("..", "..", "data", "output", "temp")) == False:
        os.mkdir(os.path.join("..", "..", "data", "output", "temp"))
    if os.path.exists(os.path.join("..", "output", "group_01")) == False:
        os.mkdir(os.path.join("..", "output", "group_01"))

    #ermittle alle dayfiles (pickel Dateien im dayFiles Verzeichnis):
    dayFiles = glob.glob(os.path.join("..", "..", "data", "TUDA_data", "pickleDataGroup1", "dayFiles", "*.pickle"))

    for ii in dayFiles:
        dayFileName = ii.split("\\")[-1].split(".")[0]
        print("processing day File %s" %(dayFileName))

        df = read_data(ii)
        df_time_calc = organize_data(df)
        del df
        df_time_calc = calc_duration(df_time_calc)
        # remove rows where time exceeds time of a day!
        df_time_calc = df_time_calc[df_time_calc['diff_timestamp'] <= 86400]
        df_time_calc, days = calc_week_and_day(df_time_calc)

        wgTypes = [i for i in range(1, 9)]
        allWgTypes = []
        for wgType in wgTypes:
            df_wgType = wgTypeMovingTime(wgType, df_time_calc, days)
            allWgTypes.append(df_wgType)
        df_allWgTypesMovement = dd.multi.concat(allWgTypes, axis=0, ignore_index=True)
        result = df_allWgTypesMovement.compute()
        result.to_csv(os.path.join("..", "..", "data", "output", "temp", "result_day_"+dayFileName+".csv"))
        del result

def postproc_wgMovementOverTimePreproc():
    """Postproc the data by adding the single result files together, save them as final result file and delete the temporary result files."""
    print("running postprocessing.")
    path = os.path.join("..", "..", "data", "output", "temp","*.csv")
    df = dd.read_csv(path)

    #sortieren nach wagon Type und Day:
    df = df.map_partitions(
        lambda tmp_df: tmp_df.sort_values(by=["wagon_type", "day"])
    )
    df.compute().to_csv(os.path.join("..", "output", "group_01", "wagon_types_movent_data.csv"))
    #os.rmdir(path)



if __name__ == '__main__':
    start = time.time()
    client = Client(n_workers=8, threads_per_worker=2) # set number of cpu cores and number of threads per core here

    #preProcCsvToPickle()           #only uncomment and run this on the first time!
    #preProcDatasetsToDaysFiles()    #only uncomment and run this on the first time!

    run_processing()
    postproc_wgMovementOverTimePreproc()
    print('calculation took %f seconds.' % (time.time() - start))
    client.shutdown()
