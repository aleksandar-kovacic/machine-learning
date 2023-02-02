from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
import numpy as np
from time import time
from os.path import join, exists
from os import mkdir, remove, rmdir
from glob import glob
import h3           
import re
from tqdm import tqdm


def get_gnss_quality(df: dd.DataFrame, weight_satellite: float = 0.5) -> dd.DataFrame:
    """
    adds feature for GPS signal quality
        (1) normalize values of signal_quality_satellite and signal_quality_hdop so
            that they are between 0 and 1
        (2) weighted sum of signal_quality_satellite, signal_quality_hdop
            default weights: 0.5/0.5
        (3) if no gnss data can be transmitted, signal_quality = 0
    :param weight_satellite: weight for signal_quality_satellite for weighted sum
    :param df: dask dataframe containing longterm data
    :return: dask dataframe with added column for GPS signal quality
    """
    # (1) normalize data
    min_quality_satellite = df.signal_quality_satellite.min()
    max_quality_satellite = df.signal_quality_satellite.max()
    df['signal_quality_satellite'] = (df.signal_quality_satellite - min_quality_satellite) / (
            max_quality_satellite - min_quality_satellite)
    min_quality_hdop = df.signal_quality_hdop.min()
    max_quality_hdop = df.signal_quality_hdop.max()
    df['signal_quality_hdop'] = 1 - (df.signal_quality_hdop - min_quality_hdop) / (
            max_quality_hdop - min_quality_hdop)

    # (2) weighted sum
    df['GNSS_quality'] = df['signal_quality_satellite'] * weight_satellite + \
                         df['signal_quality_hdop'] * (1 - weight_satellite)

    # (3) fillna
    df['GNSS_quality'] = df['GNSS_quality'].fillna(0)
    return df

def read_data(path: str) -> dd.DataFrame:
    return dd.read_parquet(path,
                          columns=['wagon_ID','signal_quality_satellite', 'signal_quality_hdop',
                                   'timestamp_index', 'timestamp_transfer', 'timestamp_measure_position',
                                   'latitude', 'longitude', 'determination_position', 'provider']
                          )

def preproc_data(ddf: dd.DataFrame) -> dd.DataFrame:
    # drop signal_quality_hdop >= 10
    ddf.map_partitions(lambda df: df.drop( df[df.signal_quality_hdop >= 10].index, axis=0, inplace=True))
    # drop signal_quality_satellite > 10
    ddf.map_partitions(lambda df: df.drop( df[df.signal_quality_satellite > 10].index, axis=0, inplace=True))

    # add features
    ddf = get_gnss_quality(ddf)
    ddf["mobile_quality"] = ddf["timestamp_transfer"] - ddf["timestamp_measure_position"]
    ddf["latency"] = ddf["timestamp_index"] - ddf["timestamp_transfer"]

    # drop negative features
    ddf = ddf[ddf.GNSS_quality >= 0]
    ddf = ddf[ddf.mobile_quality >= 0]
    ddf = ddf[ddf.latency >= 0]

    # ignore wagon IDs (see FAQs)
    ddf = ddf[ddf.wagon_ID != 1209603222683999]
    ddf = ddf[ddf.wagon_ID != 1176603735805403]
    return ddf

def add_hexbins(ddf: dd.DataFrame, resolution: int) -> dd.DataFrame:
    # lat/long -> hexbins:
    hexbin_func = lambda df: df.apply(lambda x: h3.geo_to_h3(lat=x.latitude, lng=x.longitude, resolution=resolution), axis=1)
    ddf["hexbin"] = ddf.map_partitions(hexbin_func)
    return ddf


def cluster_data_groupby(ddf: dd.DataFrame, list_of_features: list) -> pd.DataFrame:
    # groupy and aggregate data
    dic = {}
    for it, feat in enumerate(list_of_features):
        if it==0:
            dic[feat] = ['mean', 'std', 'count']
        else:
            dic[feat] = ['mean', 'std']
    ddf = ddf.groupby(["hexbin"]).agg(dic)
    ddf = ddf.fillna(0)
    ddf = ddf.reset_index()
    df = ddf.compute()
    del ddf

    # format to final_df with mean and std of the features in each cluster (hexbin)
    dic = {"hexbin": df.hexbin}
    for it, feat in enumerate(list_of_features):
        if it==0:
            dic[feat+"_mean"] = df[feat]["mean"]
            dic[feat+"_std"] = df[feat]["std"]
            dic["count_in_hexbin"] = df[feat]["count"]
        else:
            dic[feat+"_mean"] = df[feat]["mean"]
            dic[feat+"_std"] = df[feat]["std"]

    final_df = pd.DataFrame(dic)
    return final_df


def cluster_data_add_hexbin_center(final_df: pd.DataFrame, provider:int, determination_position:int) -> pd.DataFrame or bool:
    print(f"process provider {provider} and determination_position {determination_position}")
    # add hexbin center:
    hexbin_centerpoint_func = lambda x: h3.h3_to_geo(x.hexbin)
    try:
        final_df["hexbin_center_lat"], final_df["hexbin_center_lon"] = \
            zip( *final_df.apply(hexbin_centerpoint_func, axis=1) )
        return final_df
    except:   # when final_df is empty!
        print(f"skipping determination_position={determination_position} and provider={provider}. (Because df is empty)")
        continue_Loop = True
        return continue_Loop

def cluster_data_save_output(final_df:pd.DataFrame, by_provider:bool, \
    by_determination_position:bool, resolution:int, provider:int, determination_position:int) -> None:
    # check if target directory exists:
    if not exists(join("..", "output", )):
        mkdir(join("..", "output"))
    if not exists(join("..", "output", "temp")):
        mkdir(join("..", "output", "temp"))
    # save final_df:
    if by_provider==False and by_determination_position==False:
        result_path = join("..", "output", "temp",
                "clusteredData_" +
                "Resolution="+str(resolution)+".csv")
    if by_provider==True and by_determination_position==False:
        result_path = join("..", "output", "temp",
                "clusteredData_detPos=" +
                "_Provider="+str(provider)+"_Resolution="+str(resolution)+".csv")
    if by_provider==False and by_determination_position==True:
        result_path = join("..", "output", "temp",
                "clusteredData_detPos=" + str(determination_position) +
                "_Resolution="+str(resolution)+".csv")
    if by_provider==True and by_determination_position==True:
        result_path = join("..", "output", "temp",
                "clusteredData_detPos=" + str(determination_position) +
                "_Provider="+str(provider)+"_Resolution="+str(resolution)+".csv")
    final_df.to_csv(result_path, index=False)
    return


def cluster_data(ddf:dd.DataFrame, by_provider:bool, by_determination_position:bool, \
    list_of_features: list, resolution: int):
    if by_provider==False and by_determination_position==False:
        ddf = ddf[["determination_position", "provider", "hexbin"] + list_of_features]
        final_df = cluster_data_groupby(ddf, list_of_features)
        del ddf
        provider, determination_position = None, None
        final_df = cluster_data_add_hexbin_center(final_df, provider, determination_position)
        cluster_data_save_output(final_df, by_provider, by_determination_position, \
                resolution, provider, determination_position)

    if by_provider==True and by_determination_position==False:
        ddf_origin = ddf[["determination_position", "provider", "hexbin"] + list_of_features]
        for provider in [32, 35, 37]:
            ddf = ddf_origin[(ddf_origin.provider==provider)]
            final_df = cluster_data_groupby(ddf, list_of_features)
            del ddf
            determination_position = None
            final_df = cluster_data_add_hexbin_center(final_df, provider, determination_position)
            if isinstance(final_df, bool):
                continue
            cluster_data_save_output(final_df, by_provider, by_determination_position, \
                resolution, provider, determination_position)
        del ddf_origin

    if by_provider==False and by_determination_position==True:
        ddf_origin = ddf[["determination_position", "provider", "hexbin"] + list_of_features]
        for determination_position in [1, 3, 4]:
            ddf = ddf_origin[(ddf_origin.determination_position==determination_position)]
            final_df = cluster_data_groupby(ddf, list_of_features)
            del ddf
            provider = None
            final_df = cluster_data_add_hexbin_center(final_df, provider, determination_position)
            if isinstance(final_df, bool):
                continue
            cluster_data_save_output(final_df, by_provider, by_determination_position, \
                resolution, provider, determination_position)
        del ddf_origin

    if by_provider==True and by_determination_position==True:
        ddf_origin = ddf[["determination_position", "provider", "hexbin"] + list_of_features]
        for determination_position in [1, 3, 4]:
            for provider in [32, 35, 37]:
                ddf = ddf_origin[(ddf_origin.determination_position==determination_position) &
                        (ddf_origin.provider==provider)]
                final_df = cluster_data_groupby(ddf, list_of_features)
                del ddf
                final_df = cluster_data_add_hexbin_center(final_df, provider, determination_position)
                if isinstance(final_df, bool):
                    continue
                cluster_data_save_output(final_df, by_provider, by_determination_position, \
                    resolution, provider, determination_position)
        del ddf_origin


def postprocess(resolution:int, by_provider:bool, by_determination_position:bool):
    # Postproc: fuse single files (provider, position_determination) to one allData file
    files = glob(join("..", "output", "temp", "*"))
    li = []
    for file in files:
        df = pd.read_csv(file)
        # append determination_position and provider as column
        if by_determination_position == True:
            detPos=list(map(int, re.findall(r'\d+', file)))[0]
            df["determination_position"] = np.ones(len(df))*detPos
        if by_provider and by_determination_position:
            provider=list(map(int, re.findall(r'\d+', file)))[1]
            df["provider"] = np.ones(len(df))*provider     
        elif by_provider:
            provider=list(map(int, re.findall(r'\d+', file)))[0]
            df["provider"] = np.ones(len(df))*provider    
        li.append(df)
#    # delete temp folder and files within:
#    files = glob(join("..", "output", "temp", "*"))
#    for f in files:
#        remove(f)
#    rmdir(join("..", "output", "temp"))
    # concat all files together
    final_df = pd.concat(li, axis=0, ignore_index=True)
    output_path = join("..", "output")
    result_path = join(output_path, "clusteredData_Resolution="+str(resolution)+ \
        "_byProvider="+str(by_provider)+"_byPosDet="+str(by_determination_position)+".csv")
    final_df.to_csv(result_path, index=False)


def process(list_of_features:list, resolution:int = 4, by_provider:bool = False, by_determination_position:bool = False) -> None:
    file_path = join("..", "..", "data", "TUDA_data", "all_TUDA_data.parquet")
    ddf = read_data(file_path)
    ddf = preproc_data(ddf)
    
    # create dataframe that only contains features for statistical analysis
    #features_df = ddf[['mobile_quality', 'GNSS_quality', 'latency', 'determination_position', 'provider']]

    # add hexbins:
    ddf = add_hexbins(ddf, resolution)

    # filter relevant columns
    ddf = ddf[["determination_position", "provider", "hexbin"] + list_of_features]

    #clustering process:
    cluster_data(ddf, by_provider, by_determination_position, list_of_features, resolution)
    del ddf

    #postproc:
    postprocess(resolution, by_provider, by_determination_position)
    output_path = join("..", "output")
    #features_df.to_parquet(join(output_path, "part2_features.parquet"), write_index=False)



if __name__ == '__main__':
    start = time()
    client = Client(n_workers=8, threads_per_worker=2)  # set number of cpu cores and number of threads per core here
    # hier die zu untersuchenden features in listenform angeben:
    list_of_features = ["GNSS_quality", "mobile_quality", "latency"]
    # resolution for hexbin with h3: https://h3geo.org/docs/core-library/restable

    # OPTIONS:  resolution: hexbin resolution (no. of hexbins),
    #           by_provider/by_determination_position: wether distiguish between providers / determination_position or not

    # process for one setting:
    process(list_of_features, resolution=5, by_provider=True, by_determination_position=True)

#    # process for multiple settings:
#    by_provider_ls = [True, False]
#    by_determination_position_ls = [True, False]
#    resolutions = [4, 5, 6]
#    for resolution in tqdm(resolutions):
#        for by_provider in tqdm(by_provider_ls):
#            for by_determination_position in tqdm(by_determination_position_ls):
#                process(list_of_features, resolution=resolution, by_provider=by_provider, by_determination_position=by_determination_position)
 
    print('calculation took %f seconds.' % (time() - start))
    client.shutdown()
