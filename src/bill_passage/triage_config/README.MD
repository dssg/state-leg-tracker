### Experiment configuration files

This folder contains configuration files that we use for running triage experiments for Bill passage

- `national_long_term_master.yaml`: The experiment config for predicting long term (1 year) bill passage likelihood at national level. 

- `national_short_term_master.yaml`: The experiment config for predicting short term (1 month) bill passage likelihood at national level


Currently we are focusing more on the long term prediction. The table below shows a small description of the experiments we have run so far with their respective triage assigned experiment hashes.

Experiment Hash | Description 
--- | --- 
`dc52ec9e21af7cceb2abd22e876040dd` | Complete experiment with 6month model update freq. Used for initial model calibration 
`170870efbfa049eff695a0423dfe5ea8` | Experiment with a 3month model update frequency, sponsor success features, and Percentile rankers (sponsor success) for baselines




