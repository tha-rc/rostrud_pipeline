# Rostrud data collection pipeline

## Pipeline explained

This repo contains a pipeline script that collects resume/job vacancy data from the open data portal [Jobs in Russia](https://trudvsem.ru/) and puts it into the Postgres database in a quasi-normalized form.

The main file is `pipline.py`, there are two parameters inside it:
* monthly=True - collect the archive only for the last day of each available month. 
* remove_gz=True - subsequent deletion of all downloaded archives.

Access to the database should be configured in `/src/config_to_bd.yml`.
The name of the temporary folder, e.g. workdir, must be specified in the `working_directory: "./workdir"` line in `all_tables_names.yml`.

## Datasets preparation

There are two data preparation SQL/py scripts located in the `datasets` folder that create cleaned aggregates for:
1) A dataset on educational and career trajectories
2) A dataset on the activity of unemployed candidates

The execution order:
- execute `dataset1.sql` and `dataset2.sql` to create a raw data table
- manually export the tables to `dataset1.csv` and `dataset2.csv` with headers
- execute `dataset1.py` and `dataset2.py` to get the cleaned versions, namely `dataset1.csv.clean.csv` and `dataset2.csv.clean.csv`
- execute `dataset1.check.py` and `dataset2.check.py` to finalize datasets
- execute `dataset2.clusters.py` to add the cluster column

## License

We adapted a pipeline that was originally developed by researchers from [CPUR](https://cpur.pro/en)/[INID](https://web.archive.org/web/20220124060237/https://www.data-in.ru/).

All raw data available in the source are distributed in the public domain and are completely free of charge based on the principles of use by the Russian Government ([see](https://github.com/tha-rc/rostrud_pipeline/blob/main/misc/protokol2016.pdf)). The data that was obtained as a result of our scripts are distributed under CC-BY-4.0 ([see](https://github.com/tha-rc/rostrud_pipeline/blob/main/misc/CC-BY-4.0.txt)). All code is provided under the MIT license ([see](https://github.com/tha-rc/rostrud_pipeline/blob/main/misc/MIT.txt)) and can also be used freely.

## References

- Valko, D., Vasilevskaia, M., Bunina, M., Kozlova, M., Filippova, A. M., & Rud, D. (2024). Educational and Career Trajectories in Russia: Open Datasets. Zenodo. https://doi.org/10.5281/zenodo.10913325
