# rostrud data collection pipeline

## Pipeline explained

This repo contains a pipeline script that collects resume/job vacancy data from the open data portal [Jobs in Russia](https://trudvsem.ru/) and puts it into the Postgress database in a quasi-normalized form.

The main file is `pipline.py`, there are two parameters inside it:
* monthly=True - collect the archive only for the last day of each available month. 
* remove_gz=True - subsequent deletion of all downloaded archives.
Access to the database should be configured in `/src/config_to_bd.yml`.
The name of the temporary folder, e.g. workdir, must be specified in the `working_directory: "workdir"` line in `all_tables_names.yml`.

## Datasets preparation

There are two data preparation SQL scripts located in the `datasets` folder that create cleaned aggregates for:
* 1) A dataset on the activities of unemployed candidates
* 2) A dataset on education and career trajectories


## License

All raw data available in the source are distributed in the public domain and are completely free of charge based on the principles of use by the Russian Government ([see](https://github.com/Ellariel/rostrud_pipeline/blob/main/misc/protokol2016.pdf)). The data that was obtained as a result of our scripts are distributed under CC-BY-4.0 ([see](https://github.com/Ellariel/rostrud_pipeline/blob/main/misc/CC-BY-4.0.txt)). All code is provided under the MIT license ([see](https://github.com/Ellariel/rostrud_pipeline/blob/main/MIT.txt)).