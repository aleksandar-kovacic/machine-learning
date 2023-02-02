# Importieren der benötigten Bibliotheken
from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
import numpy as np
import time
from dask_jobqueue import SLURMCluster
import os


def read_data() -> dd.DataFrame:
    """
    Alle Daten im parquet Format werden eingelesen (all_TUDA_data.parquet), die im Skript "02_general_preprocessing.py" erzeugt / preprocessed werden
    :return: dask.DataFrame
    """
    # Einlesen der Daten
    filename = "all_TUDA_data.parquet"
    # Pfad zu den Daten
    file_path = os.path.join("..", "..", "data", "TUDA_data", filename)
    df = dd.read_parquet(file_path)
    # Reihen mit "NaN" in "movement_state" werden vernachlässigt, Begründung siehe Dokumentation
    df = df.dropna(subset=["movement_state"])
    return df


def organize_data(df: dd.DataFrame) -> dd.DataFrame:
    """
    Daten, die nicht benötigt werden werden vernachlässigt,
    DataFrane wird nach "wagonID" und "timestamp (timestamp_measure_movement_state)" sortiert
    :param df: dask.DataFrame
    :return: sortierter und gefilterter dask.DataFrame
    """
    # Relevante Daten werden behalten
    df_time_calc = df[["wagon_ID", "movement_state", "wagon_type", "timestamp_measure_movement_state"]]
    # Sortierung des DataFrames
    df_time_calc = df_time_calc.map_partitions(
        lambda tmp_df: tmp_df.sort_values(by=["wagon_ID", "timestamp_measure_movement_state"])
    )
    return df_time_calc


def calc_week(df_time_calc: dd.DataFrame) -> dd.DataFrame:
    """
    Spalte mit der Angabe der Woche (absolut) des Datenpunktes wird hinzugefügt
    :param df: dask.DataFrame
    :return: dask.DataFrame mit hinzugefügter Spalte "Woche"
    """
    # Berechnen eines Tages aus "timestamp (timestamp_measure_movement_state)"
    df_time_calc['Tag'] = (df_time_calc['timestamp_measure_movement_state'] / (24 * 60 * 60))
    df_time_calc['Tag'] = df_time_calc['Tag'].astype("int")
    # Erstellen von "Bins" der jeweiligen Wochen
    bins_weeks = np.arange(0, 50 * 7 * 24 * 60 * 60, 7 * 24 * 60 * 60)
    # Liste mit Labels für die "Bins" für jede Woche erstellen
    bins_weeks_labels = [i + 1 for i, _ in enumerate(bins_weeks)]
    # Bins werden aus einem numpy array in Liste umgewandelt
    bins_weeks = bins_weeks.tolist()
    # Letzter Wert für die Labels wird gelöscht, da ein Label mehr als Bins erstellt wird (Bin ist jeweils zwischen zwei Zeitpunkten definiert)
    bins_weeks_labels.pop()
    # Daten mittels "timestamp (timestamp_measure_movement_state)" in die erstellten Bins einteilen
    # Label ("Woche") wird dem DataFrame angehängt
    df_time_calc['Woche'] = df_time_calc.timestamp_measure_movement_state.map_partitions(
        pd.cut, bins=bins_weeks, labels=bins_weeks_labels
    )
    return df_time_calc


def calc_duration(df_time_calc: dd.DataFrame) -> dd.DataFrame:
    """
    Berechnet die Dauer eines Bewegungsvorgangs (Movement Operation) anhand der
    Differenzen der Zeitstempel zwischen den verschiedenen Bewegungszuständen,
    fügt dem Datenrahmen die Spalte "diff_timestamp" hinzu.
    :param df: dask.DataFrame
    :return: dask.DataFrame mit hinzugefügter Spalte "diff_timestamp"
    """
    df_time_calc['diff_timestamp'] = df_time_calc['timestamp_measure_movement_state'].diff(periods=-1)
    condition = (df_time_calc.wagon_ID.shift(periods=-1) != df_time_calc.wagon_ID)
    df_time_calc.diff_timestamp = df_time_calc.diff_timestamp.mask(condition, 0)
    df_time_calc.diff_timestamp = df_time_calc.diff_timestamp.abs()
    return df_time_calc


def calc_efficiency(df_wg_type: dd.DataFrame, max_week=None):
    """
    berechnet die Bewegungseffizienz (z. B. Bewegungszeit / Gesamtzeit)
    die Bewegungszustände sind: Parken(0), Stehen(1), Bewegen(2)
    :param df: dask DataFrame
    :param wagon_type: int wagon type (beschreibt den Wagontyp)
    :param max_week: optionaler Parameter, der die maximale Woche beschreibt
    :return total_efficiency: float movement_time/total_time
    :return efficiency_moving_standing: float movement_time/(movement_time + standing_time)
    :return efficiency_moving_parking: float movement_time/(movement_time + parking_time)
    :return sums_mov_states[2]: float movement time
    """
    sums_mov_states = [[], [], []]
    if max_week is not None:
        grouped_series = df_wg_type.groupby(by=['Woche', 'movement_state']).diff_timestamp.sum().compute()
        for week in range(max_week):
            this_sums = grouped_series.xs(week + 1, level='Woche').to_numpy()
            for idx in range(3):
                sums_mov_states[idx].append(this_sums[idx])
    else:
        sums_mov_states = df_wg_type.groupby(by=['movement_state']).diff_timestamp.sum().compute().to_numpy()

    total_efficiency = np.asarray(sums_mov_states[2]) / (np.asarray(sums_mov_states).sum(axis=0)) * 100  # Einheit in Prozent [%]
    # Effizient bezogen auf die Standzeit = (UPTIME (move))/(UPTIME (move) + DOWNTIME(stand))
    efficiency_moving_standing = np.asarray(sums_mov_states[2]) / (np.asarray(sums_mov_states[2]) + np.asarray(sums_mov_states[1])) * 100
    # Effizient bezogen auf die Standzeit = (UPTIME (move))/(UPTIME (move) + DOWNTIME(park))
    efficiency_moving_parking = np.asarray(sums_mov_states[2]) / (np.asarray(sums_mov_states[2]) + np.asarray(sums_mov_states[0])) * 100
    return total_efficiency, efficiency_moving_standing, efficiency_moving_parking, sums_mov_states[2]


def save_dataframe(max_week: int,
                   list_for_plot_total_efficiency: list,
                   list_for_plot_efficiency_moving_standing: list,
                   list_for_plot_efficiency_moving_parking: list,
                   lists_movement_duration: list,
                   list_for_plot_total_efficiency_for_week: list,
                   list_for_plot_efficiency_moving_standing_for_week: list,
                   list_for_plot_efficiency_moving_parking_for_week: list):
    print('pandas DataFrame efficiency ...\n')
    df_efficiency = pd.DataFrame({'wagon_types': list(np.arange(1, 9, 1)),
                                  'total_efficiency': list_for_plot_total_efficiency,
                                  'efficiency_moving_standing': list_for_plot_efficiency_moving_standing,
                                  'efficiency_moving_parking': list_for_plot_efficiency_moving_parking,
                                  'list_mov_time': lists_movement_duration
                                  })
    print('completed! \n')
    max_week_for_df = np.zeros(len(list_for_plot_total_efficiency_for_week))
    max_week_for_df[0] = max_week
    print('pandas DataFrame efficiency per week ...\n')
    df_efficiency_per_week = pd.DataFrame({'max_week': max_week_for_df,
                                           'total_efficiency': list_for_plot_total_efficiency_for_week,
                                           'efficiency_moving_standing': list_for_plot_efficiency_moving_standing_for_week,
                                           'efficiency_moving_parking': list_for_plot_efficiency_moving_parking_for_week
                                           })
    print('completed! \n')
    
    # Wenn Berechnung auf Cluster: (Relativpfad führt zu Problemen mit den Schreibrechten)
    #pathToOutput1 = os.path.join("/home", "ms97cumi", "work", "01_MLA_project", "data", "output", "movement_efficiency_per_week.csv")
    #pathToOutput2 = os.path.join("/home", "ms97cumi", "work", "01_MLA_project", "data", "output", "movement_efficiency.csv")

    # Wenn Berechnung auf PC:
    pathToOutput1 = os.path.join("..", "..", "data", "output", "movement_efficiency_per_week.csv")
    pathToOutput2 = os.path.join("..", "..", "data", "output", "movement_efficiency.csv")

    print('begin saving ...\n')
    df_efficiency_per_week.to_csv(pathToOutput1)
    df_efficiency.to_csv(pathToOutput2)


def run_processing():
    """
    Ruft die bereits definierten Funktionen auf und führt diese hintereinander aus,
    speichert Resultate ab
    """
    print('reading in data ...')
    df = read_data()
    print('reading data completed! \n')
    print('organising data ...')
    df_time_calc = organize_data(df)
    print('organising data completed! \n')
    print('calc week ...')
    df_time_calc = calc_week(df_time_calc)
    print('calc week completed! \n')
    print('calc duration ...')
    df_time_calc = calc_duration(df_time_calc)
    print('calc duration completed! \n')
    lists_efficiencies = [[], [], [], []]
    lists_efficiencies_week = [[], [], []]
    
    print('calc efficiencies ...')
    for i in range(8):
        print('computing efficiencies for wagon type %i ...' % (i+1))
        df_wagon_type = df_time_calc[df_time_calc["wagon_type"] == i+1]
        efficiencies = calc_efficiency(df_wagon_type)
        for eff_idx, eff in enumerate(efficiencies):
            lists_efficiencies[eff_idx].append(eff)
    print('calc efficiencies completed! \n')

    print('calc max_week ...')
    max_week = df_time_calc.Woche.max()
    max_week = max_week.compute()
    print('calc max_week completed! \n')    
    
    print('calc efficiencies 2 ...')
    for j in range(8):
        print('computing efficiencies for wagon type %i' % (j + 1))
        df_wagon_type = df_time_calc[df_time_calc["wagon_type"] == j + 1]
        efficiencies = calc_efficiency(df_wagon_type, max_week= max_week)
        for eff_idx, eff in enumerate(efficiencies[:-1]):
            lists_efficiencies_week[eff_idx].append(eff)
    print('calc efficiencies 2 completed! \n')

    print('saving the data ...\n')
    save_dataframe(max_week,
                   lists_efficiencies[0], lists_efficiencies[1], lists_efficiencies[2], lists_efficiencies[3],
                   lists_efficiencies_week[0], lists_efficiencies_week[1], lists_efficiencies_week[2])


if __name__ == '__main__':
    # Wenn auf dem Cluster:
    #cluster = SLURMCluster(header_skip=['--mem=', '--cpus', '-n 1'], scheduler_options={'dashboard_address': ':4500'}, job_extra=['--mem-per-cpu=3800', r"-e /work/scratch/ms97cumi/01_MLA_project/logs/%x.err.%", r"-o /work/scratch/ms97cumi/01_MLA_project/logs/%x.out.%j", '-n 96'], memory ="300 GB", cores=96, project="project01526", walltime="00:30:00", interface='ib0', shebang="#!/bin/bash", n_workers=4)
    #print(cluster.job_script())
    #cluster.adapt(minimum_jobs=4, maximum_jobs=6)
    #print(cluster)
    #start = time.time()
    #client = Client(cluster)
    #run_processing()
    #print(time.time() - start)
    #cluster.close()
    #client.shutdown()

    # Wenn auf PC:
    start = time.time()
    # n_workers und threads_per_worker an PC spezifizieren
    client = Client(n_workers=4, threads_per_worker=2)
    run_processing()
    print(time.time() - start)
    client.shutdown()