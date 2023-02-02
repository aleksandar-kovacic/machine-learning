import pandas as pd
import numpy as np
import os
import h3

def format_timestamp(timestamp_string: str) -> float:
    """
    Funktion, die aus dem timestamp_string einen float in sekunden zurückgibt
    :param timestamp_string: Zeitstempel
    :return: float: Zeit in Sekunden
    """
    string_list_leerzeichen = timestamp_string.split(' ')
    days = float(string_list_leerzeichen[0])
    days_in_sec = days * 24 * 60 * 60
    string_list_doppelpunkt = string_list_leerzeichen[-1].split(':')
    hours = float(string_list_doppelpunkt[0])
    hours_in_sec = hours * 60 * 60
    minutes = float(string_list_doppelpunkt[1])
    minutes_in_sec = minutes * 60
    sec_in_sec = float(string_list_doppelpunkt[2])
    total_time_in_sec = days_in_sec + hours_in_sec + minutes_in_sec + sec_in_sec
    return total_time_in_sec


def df_into_feature_df_by_provider(df, feature):
    df_prov_32 = df[(df["provider"] == 32) ]
    df_prov_35 = df[(df["provider"] == 35) ]
    df_prov_37 = df[(df["provider"] == 37) ]

    hexbins = pd.concat([df_prov_32.hexbin, df_prov_32.hexbin, df_prov_32.hexbin])
    hexbins = hexbins.unique()

    df_out = pd.DataFrame(data={ "hexbin": hexbins })
    df_out = pd.merge(df_out, df_prov_32[[feature,"hexbin"]], how="outer", on="hexbin")
    df_out = pd.merge(df_out, df_prov_35[[feature,"hexbin"]], how="outer", on="hexbin")
    df_out = pd.merge(df_out, df_prov_37[[feature,"hexbin"]], how="outer", on="hexbin")

    df_out = df_out.rename(columns={
        "hexbin":"hexbin", feature+"_x": feature+"_prov_32", feature+"_y": feature+"_prov_35", feature: feature+"_prov_37", 
    })

    return df_out



def df_provder_make_equal_quantiles(df, feature ,no_quantiles=4):
    cols_list = ["prov_32", "prov_35", "prov_37"]
    cols_list = [feature+"_"+x for x in cols_list]

    min, max = df[cols_list].min().min(), \
        df[cols_list].max().max()

    quantiles = np.linspace(min, max, no_quantiles+1)

    max_quantile_len = 0
    # get length of the longest quantile
    for prov in cols_list:
        df_temp = df[prov]
        for i in range(no_quantiles):
            quantile_len = len(df_temp[ (df_temp >= quantiles[i]) & (df_temp < quantiles[i+1]) ] )
            if quantile_len > max_quantile_len:
                max_quantile_len = quantile_len


    ### fill up data for equal quantile lenghts
    df_list = []
    # add min/max and quentile values themselfs
    for prov in cols_list:
        df_prov = df[["hexbin", prov]]
        df_prov = df_prov.dropna()

        df_prov = pd.concat( [
                    df_prov, pd.DataFrame(data={"hexbin": [h3.geo_to_h3(0, 0, 12)]*2, prov: [min,max] })
                    ] )
        df_prov = pd.concat( [
                    df_prov, pd.DataFrame(data={"hexbin": [h3.geo_to_h3(0, 0, 12)]*(no_quantiles+1), prov: quantiles })
                    ] )

    ## add additional data for shifting quantiles          
        #print()
        for i in range(no_quantiles):
            df_temp = df_prov[prov]
            quantile_len = len(df_temp[ (df_temp >= quantiles[i]) & (df_temp < quantiles[i+1]) ] )
            if i == range(no_quantiles)[-1]:
                quantile_len = len(df_temp[ (df_temp >= quantiles[i]) ] )
            #print(quantile_len)

            if quantile_len < max_quantile_len+500:
                diff_quantile_len = max_quantile_len+500 -quantile_len
                
                hexbins = np.ones(diff_quantile_len, dtype=object)
                hexbins = hexbins*h3.geo_to_h3(0, 0, 12)
                values = np.ones(diff_quantile_len, dtype=float)
                values = values*0.5*(quantiles[i]+quantiles[i+1])

                df_temp = pd.DataFrame(data={"hexbin": hexbins, prov: values})
                df_prov = pd.concat([df_prov, df_temp])

    ## uniform data in each quantile to the same value
        for i in range(no_quantiles):
            if i != range(no_quantiles)[-1]:
                df_prov[prov] = np.where( ((df_prov[prov] >= quantiles[i]) & (df_prov[prov] < quantiles[i+1])), 0.5*(quantiles[i]+quantiles[i+1]), df_prov[prov])
            else:
                df_prov[prov] = np.where( ((df_prov[prov] >= quantiles[i])), 0.5*(quantiles[i]+quantiles[i+1]), df_prov[prov])


        df_list.append(df_prov)

    return df_list
