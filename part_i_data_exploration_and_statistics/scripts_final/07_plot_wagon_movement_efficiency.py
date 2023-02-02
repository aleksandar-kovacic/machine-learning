### Imports
import glob
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#import matplotlib as mpl
#mpl.rcParams.update(mpl.rcParamsDefault)

#Einheitlicher Plot Style
plt.style.use(['mystyle'])

# Einlesen der Liste für das Plotten
fileName = "movement_efficiency"    # hier entsprechenden Dateinamen auf die gewünschte .csv Datei angeben
filenamePath = os.path.join("..", "output", fileName + ".csv") 

# Dateien einlesen: pd.read_csv, um csv einzulesen
df = pd.read_csv(filenamePath)

#DataFrame wird in einzelne Spalten aufgeteilt, die dann als Liste unterschiedliche Variablen darstellen und zum Plotten verwendet werden.
list_for_plot_total_efficiency = df["total_efficiency"].to_list()
list_for_plot_efficiency_moving_standing = df["efficiency_moving_standing"].to_list()
list_for_plot_efficiency_moving_parking = df["efficiency_moving_parking"].to_list()

#Plotten der Gesamteffizienz und der relativen Effizienzen (Moving vs Standing und Moving vs Parking) bezogen auf die 8 Wagontypes
x_wagon_type = range(1,9)
y_wagon_type_total_efficiency = np.around(np.array(list_for_plot_total_efficiency), decimals = 2)
y_wagon_efficiency_moving_standing = np.around(np.array(list_for_plot_efficiency_moving_standing), decimals = 2)
y_wagon_efficiency_moving_parking = np.around(np.array(list_for_plot_efficiency_moving_parking), decimals = 2)

fig_1 = plt.figure(figsize=(12,10), constrained_layout=True)

ax11 = fig_1.add_subplot(331)
ax11.bar(x_wagon_type, y_wagon_type_total_efficiency)
ax11.set(xlabel = "Wagontyp", ylabel = "Gesamteffizienz [\%] \n[Moving Time/(Moving Time + Standing Time + Parking Time)]")
#show values on bar plot
for i, v in enumerate(y_wagon_type_total_efficiency):
    ax11.text(i + 0.65, v+0.1, str(v))

ax21 = fig_1.add_subplot(332)
ax21.bar(x_wagon_type, y_wagon_efficiency_moving_standing)
ax21.set(xlabel = "Wagontyp", ylabel = "Moving vs. Standing Effizienz [\%] \n[Moving Time/(Moving Time + Standing Time)]")
for i, v in enumerate(y_wagon_efficiency_moving_standing):
    ax21.text(i + 0.57, v+0.27, str(v))

ax31 = fig_1.add_subplot(333)
ax31.bar(x_wagon_type, y_wagon_efficiency_moving_parking)
ax31.set(xlabel = "Wagontyp", ylabel = "Moving vs. Parking Effizienz [\%] \n[Moving Time/(Moving Time + Parking Time)]")
for i, v in enumerate(y_wagon_efficiency_moving_parking):
    ax31.text(i + 0.65, v+0.1, str(v))

ax11.grid(False)
ax21.grid(False)
ax31.grid(False)


#Plotten der gesamten Movement Time der einzelnen Wagons. Hierbei wurde die Dauer der Movement States zusammenaddiert und für jeden Wagontype geplottet.
list_for_move_time = df["list_mov_time"].to_list()

x_wagon_type = range(1,9)
list_for_move_time = np.array(list_for_move_time)
y_wagon_move_time = np.around(list_for_move_time/np.amax(list_for_move_time), decimals = 3)

fig_3 = plt.figure(figsize=(11,10), constrained_layout=True)
ax13 = fig_3.add_subplot(331)
ax13.bar(x_wagon_type, y_wagon_move_time)
ax13.set(xlabel = "Wagontyp", ylabel = "Movement Time (normiert auf max. Wert)")
ax13.xaxis.set(ticks=range(1,9))
ax13.grid(False)
#Show values on bar plot
for i, v in enumerate(y_wagon_move_time):
    ax13.text(i + 0.6, v+0.005, str(v))




# Einlesen der Liste movement_efficiency_per_week für das Plotten
fileName = "movement_efficiency_per_week"    # hier entsprechenden Dateinamen auf die gewünschte .csv Datei angeben
filenamePath = os.path.join("..", "output", fileName + ".csv")

# Dateien einlesen: pd.read_csv, um vsc einzulesen
df = pd.read_csv(filenamePath)

##DataFrame wird in einzelne Spalten aufgeteilt, die dann als Liste unterschiedliche Variablen darstellen und zum Plotten verwendet werden.
list_for_plot_total_efficiency_for_week = df["total_efficiency"].to_list()
list_for_plot_efficiency_moving_standing_for_week = df["efficiency_moving_standing"].to_list()
list_for_plot_efficiency_moving_parking_for_week = df["efficiency_moving_parking"].to_list()

#Plotten des gesamten Effizienzkoeffizienten pro Wagon über den Zeitverlauf in Wochen
fig_2 = plt.figure(figsize=(10,6), constrained_layout=True)
fig_2.subplots_adjust(wspace=0.3, hspace=0.3)
x_week = range(1,46)

markersize = 3

#Wagon 1
list_w1_1 = list_for_plot_total_efficiency_for_week[0].split()
list_w1_2 = [s.replace("[", "") for s in list_w1_1]
list_w1_3 = [t.replace("]", "") for t in list_w1_2]
list_w1_4 = list(np.float_(list_w1_3))[:45]

y_total_efficiency_for_week = list_w1_4
ax21 = fig_2.add_subplot(2,4,1)
ax21.plot(x_week, y_total_efficiency_for_week, marker = "o", markersize=markersize)
ax21.set(title = "Wagontyp 1", ylabel = "Gesamteffizienz [\%]")
ax21.set_xlim(0,50)

#Wagon 2
list_w2_1 = list_for_plot_total_efficiency_for_week[1].split()
list_w2_2 = [s.replace("[", "") for s in list_w2_1]
list_w2_3 = [t.replace("]", "") for t in list_w2_2]
list_w2_4 = list(np.float_(list_w2_3))[:45]

y_total_efficiency_for_week = list_w2_4
ax22 = fig_2.add_subplot(2,4,2)
ax22.plot(x_week, y_total_efficiency_for_week, marker = "o", markersize=markersize)
ax22.set(title = "Wagontyp 2")
ax22.set_xlim(0,50)

#Wagon 3
list_w3_1 = list_for_plot_total_efficiency_for_week[2].split()
list_w3_15 = list_w3_1[1:]
list_w3_2 = [s.replace("[", "") for s in list_w3_15]
list_w3_3 = [t.replace("]", "") for t in list_w3_2]
list_w3_4 = list(np.float_(list_w3_3))[:45]

y_total_efficiency_for_week = list_w3_4
ax23 = fig_2.add_subplot(2,4,3)
ax23.plot(x_week, y_total_efficiency_for_week, marker = "o", markersize=markersize)
ax23.set(title = "Wagontyp 3")
ax23.set_xlim(0,50)

#Wagon 4
list_w4_1 = list_for_plot_total_efficiency_for_week[3].split()
list_w4_15 = list_w4_1[1:]
list_w4_2 = [s.replace("[", "") for s in list_w4_15]
list_w4_3 = [t.replace("]", "") for t in list_w4_2]
list_w4_4 = list(np.float_(list_w4_3))[:45]

y_total_efficiency_for_week = list_w4_4
ax24 = fig_2.add_subplot(2,4,4)
ax24.plot(x_week, y_total_efficiency_for_week, marker = "o", markersize=markersize)
ax24.set(title = "Wagontyp 4")
ax24.set_xlim(0,50)

#Wagon 5
list_w5_1 = list_for_plot_total_efficiency_for_week[4].split()
list_w5_2 = [s.replace("[", "") for s in list_w5_1]
list_w5_3 = [t.replace("]", "") for t in list_w5_2]
list_w5_4 = list(np.float_(list_w5_3))[:45]

y_total_efficiency_for_week = list_w5_4
ax25 = fig_2.add_subplot(2,4,5)
ax25.plot(x_week, y_total_efficiency_for_week, marker = "o", markersize=markersize)
ax25.set(title = "Wagontyp 5", xlabel = "Woche", ylabel = "Gesamteffizienz [\%]")
ax25.set_xlim(0,50)

#Wagon 6
list_w6_1 = list_for_plot_total_efficiency_for_week[5].split()
list_w6_2 = [s.replace("[", "") for s in list_w6_1]
list_w6_3 = [t.replace("]", "") for t in list_w6_2]
list_w6_4 = list(np.float_(list_w6_3))[:45]

y_total_efficiency_for_week = list_w6_4
ax26 = fig_2.add_subplot(2,4,6)
ax26.plot(x_week, y_total_efficiency_for_week, marker = "o", markersize=markersize)
ax26.set(title = "Wagontyp 6", xlabel = "Woche")
ax26.set_xlim(0,50)

#Wagon 7
list_w7_1 = list_for_plot_total_efficiency_for_week[6].split()
list_w7_15 = list_w7_1[:46]
list_w7_2 = [s.replace("[", "") for s in list_w7_15]
list_w7_3 = [t.replace("]", "") for t in list_w7_2]
list_w7_4 = list(np.float_(list_w7_3))[:45]

y_total_efficiency_for_week = list_w7_4
ax27 = fig_2.add_subplot(2,4,7)
ax27.plot(x_week, y_total_efficiency_for_week, marker = "o", markersize=markersize)
ax27.set(title = "Wagontyp 7", xlabel = "Woche")
ax27.set_xlim(0,50)

#Wagon 8
list_w8_1 = list_for_plot_total_efficiency_for_week[7].split()
list_w8_15 = list_w8_1[1:]
list_w8_2 = [s.replace("[", "") for s in list_w8_15]
list_w8_3 = [t.replace("]", "") for t in list_w8_2]
list_w8_4 = list(np.float_(list_w8_3))[:45]

y_total_efficiency_for_week = list_w8_4
ax28 = fig_2.add_subplot(2,4,8)
ax28.plot(x_week, y_total_efficiency_for_week, marker = "o", markersize=markersize)
ax28.set(title = "Wagontyp 8", xlabel = "Woche")
ax28.set_xlim(0,50)

ax21.grid(False)
ax22.grid(False)
ax23.grid(False)
ax24.grid(False)
ax25.grid(False)
ax26.grid(False)
ax27.grid(False)
ax28.grid(False)
plt.grid(False)



#Plotten des relativen Effizienzkoeffizienten Moving vs. Standing pro Wagon über den Zeitverlauf in Wochen
fig_4 = plt.figure(figsize=(10,6), constrained_layout=True)
fig_4.subplots_adjust(wspace=0.3, hspace=0.3)
x_week = range(1,46)

#Wagon 1
list_w1_1 = list_for_plot_efficiency_moving_standing_for_week[0].split()
list_w1_2 = [s.replace("[", "") for s in list_w1_1]
list_w1_3 = [t.replace("]", "") for t in list_w1_2]
list_w1_4 = list(np.float_(list_w1_3))[:45]

y_efficiency_moving_standing = list_w1_4
ax41 = fig_4.add_subplot(2,4,1)
ax41.plot(x_week, y_efficiency_moving_standing, marker = "o", markersize=markersize)
ax41.set(title = "Wagontyp 1", ylabel = "Effizienz Moving vs Standing [\%]")
ax41.set_xlim(0,50)

#Wagon 2
list_w2_1 = list_for_plot_efficiency_moving_standing_for_week[1].split()
list_w2_2 = [s.replace("[", "") for s in list_w2_1]
list_w2_3 = [t.replace("]", "") for t in list_w2_2]
list_w2_4 = list(np.float_(list_w2_3))[:45]

y_efficiency_moving_standing = list_w2_4
ax42 = fig_4.add_subplot(2,4,2)
ax42.plot(x_week, y_efficiency_moving_standing, marker = "o", markersize=markersize)
ax42.set(title = "Wagontyp 2")
ax42.set_xlim(0,50)

#Wagon 3
list_w3_1 = list_for_plot_efficiency_moving_standing_for_week[2].split()
list_w3_2 = [s.replace("[", "") for s in list_w3_1]
list_w3_3 = [t.replace("]", "") for t in list_w3_2]
list_w3_4 = list(np.float_(list_w3_3))[:45]

y_efficiency_moving_standing = list_w3_4
ax43 = fig_4.add_subplot(2,4,3)
ax43.plot(x_week, y_efficiency_moving_standing, marker = "o", markersize=markersize)
ax43.set(title = "Wagontyp 3")
ax43.set_xlim(0,50)

#Wagon 4
list_w4_1 = list_for_plot_efficiency_moving_standing_for_week[3].split()
list_w4_2 = [s.replace("[", "") for s in list_w4_1]
list_w4_3 = [t.replace("]", "") for t in list_w4_2]
list_w4_4 = list(np.float_(list_w4_3))[:45]

y_efficiency_moving_standing = list_w4_4
ax44 = fig_4.add_subplot(2,4,4)
ax44.plot(x_week, y_efficiency_moving_standing, marker = "o", markersize=markersize)
ax44.set(title = "Wagontyp 4")
ax44.set_xlim(0,50)

#Wagon 5
list_w5_1 = list_for_plot_efficiency_moving_standing_for_week[4].split()
list_w5_2 = [s.replace("[", "") for s in list_w5_1]
list_w5_3 = [t.replace("]", "") for t in list_w5_2]
list_w5_4 = list(np.float_(list_w5_3))[:45]

y_efficiency_moving_standing = list_w5_4
ax45 = fig_4.add_subplot(2,4,5)
ax45.plot(x_week, y_efficiency_moving_standing, marker = "o", markersize=markersize)
ax45.set(title = "Wagontyp 5", xlabel = "Woche", ylabel = "Effizienz Moving vs Standing [\%]")
ax45.set_xlim(0,50)

#Wagon 6
list_w6_1 = list_for_plot_efficiency_moving_standing_for_week[5].split()
list_w6_2 = [s.replace("[", "") for s in list_w6_1]
list_w6_3 = [t.replace("]", "") for t in list_w6_2]
list_w6_4 = list(np.float_(list_w6_3))[:45]

y_efficiency_moving_standing = list_w6_4
ax46 = fig_4.add_subplot(2,4,6)
ax46.plot(x_week, y_efficiency_moving_standing, marker = "o", markersize=markersize)
ax46.set(title = "Wagontyp 6", xlabel = "Woche")
ax46.set_xlim(0,50)

#Wagon 7
list_w7_1 = list_for_plot_efficiency_moving_standing_for_week[6].split()
list_w7_15 = list_w7_1[:46]
list_w7_2 = [s.replace("[", "") for s in list_w7_15]
list_w7_3 = [t.replace("]", "") for t in list_w7_2]
list_w7_4 = list(np.float_(list_w7_3))[:45]

y_efficiency_moving_standing = list_w7_4
ax47 = fig_4.add_subplot(2,4,7)
ax47.plot(x_week, y_efficiency_moving_standing, marker = "o", markersize=markersize)
ax47.set(title = "Wagontyp 7", xlabel = "Woche")
ax47.set_xlim(0,50)

#Wagon 8
list_w8_1 = list_for_plot_efficiency_moving_standing_for_week[7].split()
list_w8_15 = list_w8_1[:46]
list_w8_2 = [s.replace("[", "") for s in list_w8_15]
list_w8_3 = [t.replace("]", "") for t in list_w8_2]
list_w8_4 = list(np.float_(list_w8_3))[:45]

y_efficiency_moving_standing = list_w8_4
ax48 = fig_4.add_subplot(2,4,8)
ax48.plot(x_week, y_efficiency_moving_standing, marker = "o", markersize=markersize)
ax48.set(title = "Wagontyp 8", xlabel = "Woche")
ax48.set_xlim(0,50)

ax41.grid(False)
ax42.grid(False)
ax43.grid(False)
ax44.grid(False)
ax45.grid(False)
ax46.grid(False)
ax47.grid(False)
ax48.grid(False)
plt.grid(False)




#Plotten des relativen Effizienzkoeffizienten Moving vs. Parking pro Wagon über den Zeitverlauf in Wochen
fig_5 = plt.figure(figsize=(10,6), constrained_layout=True)
fig_5.subplots_adjust(wspace=0.3, hspace=0.3)
x_week = range(1,46)

#Wagon 1
list_w1_1 = list_for_plot_efficiency_moving_parking_for_week[0].split()
list_w1_2 = [s.replace("[", "") for s in list_w1_1]
list_w1_3 = [t.replace("]", "") for t in list_w1_2]
list_w1_4 = list(np.float_(list_w1_3))[:45]

y_efficiency_moving_parking = list_w1_4
ax51 = fig_5.add_subplot(2,4,1)
ax51.plot(x_week, y_efficiency_moving_parking, marker = "o", markersize=markersize)
ax51.set(title = "Wagontyp 1", ylabel = "Effizienz Moving vs Parking [\%]")
ax51.set_xlim(0,50)

#Wagon 2
list_w2_1 = list_for_plot_efficiency_moving_parking_for_week[1].split()
list_w2_15 = list_w2_1[1:]
list_w2_2 = [s.replace("[", "") for s in list_w2_15]
list_w2_3 = [t.replace("]", "") for t in list_w2_2]
list_w2_4 = list(np.float_(list_w2_3))[:45]

y_efficiency_moving_parking = list_w2_4
ax52 = fig_5.add_subplot(2,4,2)
ax52.plot(x_week, y_efficiency_moving_parking, marker = "o", markersize=markersize)
ax52.set(title = "Wagontyp 2")
ax52.set_xlim(0,50)

#Wagon 3
list_w3_1 = list_for_plot_efficiency_moving_parking_for_week[2].split()
list_w3_15 = list_w3_1[1:]
list_w3_2 = [s.replace("[", "") for s in list_w3_15]
list_w3_3 = [t.replace("]", "") for t in list_w3_2]
list_w3_4 = list(np.float_(list_w3_3))[:45]

y_efficiency_moving_parking = list_w3_4
ax53 = fig_5.add_subplot(2,4,3)
ax53.plot(x_week, y_efficiency_moving_parking, marker = "o", markersize=markersize)
ax53.set(title = "Wagontyp 3")
ax53.set_xlim(0,50)

#Wagon 4
list_w4_1 = list_for_plot_efficiency_moving_parking_for_week[3].split()
list_w4_15 = list_w4_1[1:]
list_w4_2 = [s.replace("[", "") for s in list_w4_15]
list_w4_3 = [t.replace("]", "") for t in list_w4_2]
list_w4_4 = list(np.float_(list_w4_3))[:45]

y_efficiency_moving_parking = list_w4_4
ax54 = fig_5.add_subplot(2,4,4)
ax54.plot(x_week, y_efficiency_moving_parking, marker = "o", markersize=markersize)
ax54.set(title = "Wagontyp 4", ylabel = "Effizienz Moving vs Parking [\%]")
ax54.set_xlim(0,50)

#Wagon 5
list_w5_1 = list_for_plot_efficiency_moving_parking_for_week[4].split()
list_w5_2 = [s.replace("[", "") for s in list_w5_1]
list_w5_3 = [t.replace("]", "") for t in list_w5_2]
list_w5_4 = list(np.float_(list_w5_3))[:45]

y_efficiency_moving_parking = list_w5_4
ax55 = fig_5.add_subplot(2,4,5)
ax55.plot(x_week, y_efficiency_moving_parking, marker = "o", markersize=markersize)
ax55.set(title = "Wagontyp 5", xlabel = "Woche")
ax55.set_xlim(0,50)

#Wagon 6
list_w6_1 = list_for_plot_efficiency_moving_parking_for_week[5].split()
list_w6_2 = [s.replace("[", "") for s in list_w6_1]
list_w6_3 = [t.replace("]", "") for t in list_w6_2]
list_w6_4 = list(np.float_(list_w6_3))[:45]

y_efficiency_moving_parking = list_w6_4
ax56 = fig_5.add_subplot(2,4,6)
ax56.plot(x_week, y_efficiency_moving_parking, marker = "o", markersize=markersize)
ax56.set(title = "Wagontyp 6", xlabel = "Woche")
ax56.set_xlim(0,50)

#Wagon 7
list_w7_1 = list_for_plot_efficiency_moving_parking_for_week[6].split()
list_w7_15 = list_w7_1[:46]
list_w7_2 = [s.replace("[", "") for s in list_w7_15]
list_w7_3 = [t.replace("]", "") for t in list_w7_2]
list_w7_4 = list(np.float_(list_w7_3))[:45]

y_efficiency_moving_parking = list_w7_4
ax57 = fig_5.add_subplot(2,4,7)
ax57.plot(x_week, y_efficiency_moving_parking, marker = "o", markersize=markersize)
ax57.set(title = "Wagontyp 7", xlabel = "Woche")
ax57.set_xlim(0,50)

#Wagon 8
list_w8_1 = list_for_plot_efficiency_moving_parking_for_week[7].split()
list_w8_15 = list_w8_1[1:]
list_w8_2 = [s.replace("[", "") for s in list_w8_15]
list_w8_3 = [t.replace("]", "") for t in list_w8_2]
list_w8_4 = list(np.float_(list_w8_3))[:45]

y_efficiency_moving_parking = list_w8_4
ax58 = fig_5.add_subplot(2,4,8)
ax58.plot(x_week, y_efficiency_moving_parking, marker = "o", markersize=markersize)
ax58.set(title = "Wagontyp 8", xlabel = "Woche")
ax58.set_xlim(0,50)

ax51.grid(False)
ax52.grid(False)
ax53.grid(False)
ax54.grid(False)
ax55.grid(False)
ax56.grid(False)
ax57.grid(False)
ax58.grid(False)
plt.grid(False)

plt.show()

#Zum speichern der Plots

#fig_1.patch.set_facecolor('white')
#fig_1.savefig(os.path.join("..","output","plots","group_01","000_efficiency_wagon_type.png"), \
    #bbox_inches='tight', dpi = 200,facecolor=fig_1.get_facecolor(), edgecolor='none')
#fig_1.savefig(os.path.join("..","output","plots","group_01","000_efficiency_wagon_type_new.pdf"), \
    #bbox_inches='tight', dpi = 200,facecolor=fig_1.get_facecolor(), edgecolor='none')

#fig_3.savefig(os.path.join("..","output","plots","group_01","000_test_movement_time_wagon_type.png"), \
    #bbox_inches='tight', dpi = 200,facecolor=fig_1.get_facecolor(), edgecolor='none')
#fig_3.savefig(os.path.join("..","output","plots","group_01","000_test_movement_time_wagon_type.pdf"), \
    #bbox_inches='tight', dpi = 200,facecolor=fig_1.get_facecolor(), edgecolor='none')

#fig_2.savefig(os.path.join("..","output","plots","group_01","000_total_efficiency_weeks.png"), \
    #bbox_inches='tight', dpi = 200,facecolor=fig_1.get_facecolor(), edgecolor='none')
#fig_2.savefig(os.path.join("..","output","plots","group_01","000_total_efficiency_weeks.pdf"), \
    #bbox_inches='tight', dpi = 200,facecolor=fig_1.get_facecolor(), edgecolor='none')

#fig_4.savefig(os.path.join("..","output","plots","group_01","efficiency_moving_standing_weeks.png"), \
    #bbox_inches='tight', dpi = 200,facecolor=fig_1.get_facecolor(), edgecolor='none')
#fig_4.savefig(os.path.join("..","output","plots","group_01","efficiency_moving_standing_weeks.pdf"), \
    #bbox_inches='tight', dpi = 200,facecolor=fig_1.get_facecolor(), edgecolor='none')

#fig_5.savefig(os.path.join("..","output","plots","group_01","test_efficiency_moving_parking_weeks.png"), \
    #bbox_inches='tight', dpi = 200,facecolor=fig_1.get_facecolor(), edgecolor='none')
#fig_5.savefig(os.path.join("..","output","plots","group_01","test_efficiency_moving_parking_weeks.pdf"), \
    #bbox_inches='tight', dpi = 200,facecolor=fig_1.get_facecolor(), edgecolor='none')
