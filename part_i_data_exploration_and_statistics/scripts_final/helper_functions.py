import numpy as np
import dask.dataframe as dd

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


def get_distance(part: dd.DataFrame) -> float:
    """
    Berechnet die gefahrene Distanz basierend auf den Koordinaten (latitude, longitude)
    """
    # Berechne Differenz aus den Latitüden und Longitüden
    delta_lat = part.latitude.diff(periods=1).to_numpy()[1:]
    delta_lon = part.longitude.diff(periods=1).to_numpy()[1:]

    # erstes und zweites Element jeweils für delta_lat und delta_lon
    lat1 = part.latitude.to_numpy()[0:-1:1]
    lat2 = part.latitude.to_numpy()[1::1]
    len_lat_1 = len(lat1)
    len_lat_2 = len(lat2)
    if len_lat_1 != len_lat_2:
        if len_lat_1 > len_lat_2:
            lat1 = lat1[:-1]
        else:
            lat2 = lat2[:-1]

    # Berechne Distanz
    a = np.square(np.sin(delta_lat / 2)) + np.cos(lat1) * np.cos(lat2) * np.square(np.sin(delta_lon / 2))
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(np.ones(len(a)) - a))
    distances = 6371.0 * c

    # ignoriere Distanzen, falls die Distanz zwischen zwei Messpunkten größer als 25 km ist, da kein Wagon so schnell fährt
    distances = distances[distances < 25]
    distance = sum(distances)
    return distance

# vectorized haversine function
def haversine(lat1, lon1, lat2, lon2, to_radians=True, earth_radius=6371) -> float:
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees or in radians)
    All (lat, lon) coordinates must have numeric dtypes and be of equal length.
    """
    if to_radians:
        lat1, lon1, lat2, lon2 = np.radians([lat1, lon1, lat2, lon2])

    a = np.sin((lat2-lat1)/2.0)**2 + \
        np.cos(lat1) * np.cos(lat2) * np.sin((lon2-lon1)/2.0)**2

    distance = earth_radius * 2 * np.arcsin(np.sqrt(a))

    return distance

