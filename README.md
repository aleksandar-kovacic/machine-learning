# Um den Code auszuführe:
- conda environment installieren: conda env create -f MLA_env.yml
- matplotlib plotstyle einfügen: mv mystyle.mplstyle ~/.matplotlib/stylelib/mystyle.mplstyle


#### Die Skripte sollten beginnend von part-i nach aufsteigender Numerierung durchgeführt werden, danach part-ii, part-iii.

<br></br>
# Übersicht Ordnerstruktur:
### Ablageort der Daten beachten! (data/TUDA_data ...)
<br></br>

```
MLA-project
├── MLA_conda_environment.yaml
├── MLA_plotstyle.mplstyle
│
├── data
│   ├── TUDA_battery_state
│   ├── TUDA_data
│   │   └── longterm_data
│   └── shunting_data
│
├── part_i_data_exploration_and_statistics
│   ├── output
│   │   └── output_files_of_calculation_scripts_and_plots
│   └── scripts_final
│       └── Code
│
└── part_ii and part_iii analog
```