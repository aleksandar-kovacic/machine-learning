### Importieren der verwendeten Bibilotheken
import os
import glob

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

# Einlesen des "Mapping DataFrame"
filenamePath = os.path.join("..", "..", "data", "211202_wagon_type_mapping.csv")
mappingdf = pd.read_csv(filenamePath)

# Entfernen der inneren Anführungszeichen innerhalb des strings "wagon_ID" (Parameter)
mappingdf['wagon_ID'].replace('\'', '', inplace=True, regex=True)
# Umwandeln des strings "wagon_ID" (Parameter) zu int
mappingdf['wagon_ID'] = mappingdf['wagon_ID'].astype('int64')

# Abspeichern als gepickelte .csv Datei für die weitere Verwendung
pathToPickle = os.path.join("..", "..", "data", "mappingDf.pickle")
mappingdf.to_pickle(pathToPickle)