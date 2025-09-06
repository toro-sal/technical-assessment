# Ancient Gaming Data Team Technical Assessment
This repository contains a boilerplate code structure to help you with the Data Team Technical Assessment.

**ATTENTION: DO NOT CLONE THIS REPOSITORY; INSTEAD FORK IT.**

## Table of contents
- [1. Pre-requisites](#1-pre-requisites)
- [2. Project Structure](#2-project-structure)
- [3. How to execute it](#3-how-to-execute-it)
- [4. General advice](#4-general-advice)

## 1. Pre-requisites
To run this project, you'll need:

1. Git installed and configured on your machine.
2. [Docker](https://docs.docker.com/engine/install/) installed on your machine.
3. A code editor like [VSCode](https://code.visualstudio.com/download), [Sublime Text](https://www.sublimetext.com/download) or [PyCharm](https://www.jetbrains.com/pycharm/).

## 2. Project Structure
This project structure is a suggestion. You're free to edit it to your preferences.

The project's directory tree is:
```
├─airflow
│ ├─dags
│ │ └pipeline.py
│ ├─config
│ │ └.gitkeep
│ ├─logs
│ │ └.gitkeep
│ └─plugins
│   └.gitkeep
├─data
│ ├affiliates.csv
│ ├players.csv
│ └transactions.csv
├─dbt
│ ├─analyses
│ │ └.gitkeep
│ ├─macros
│ │ └.gitkeep
│ ├─models
│ │ └─example
│ │   ├my_first_dbt_model.sql
│ │   ├my_second_dbt_model.sql
│ │   └schema.yml
│ ├─seeds
│ │ └.gitkeep
│ ├─snapshots
│ │ └.gitkeep
│ ├─tests
│ │ └.gitkeep
│ ├dbt_project.yml
│ ├profiles.yml
│ └README.md
├.gitignore
├docker-compose.yaml
├Dockerfile
├README.md
└requirements.txt
```
Inside the `airflow` folder, you'll find the `dags` folder. That's where you'll need to put the DAGs.

Inside the `data` folder, you'll find the CSVs provided with the sample data for the tables that need to be extended.

Inside the `dbt` folder is where the DBT contents are located. This part was created by running the command `dbt init` and configuring the DB connection. You'll need to edit that part to fit the challenge requests.

The Docker Compose file in this project is a light modification of the one provided by the [Apache Airflow's official documentation](https://airflow.apache.org/docs/apache-airflow/2.11.0/howto/docker-compose/index.html). It was tested and should enable an Airflow environment, a DBT CLI and a Postgres connection in a single environment, exactly what is required for this exercise.

The `.gitkeep` files do nothing. They only exist because you can't commit an empty folder. If you add any file to a folder with this file, you can delete it.

## 3. How to execute it
Open your terminal at the root folder of this repository and run:

```bash
docker-compose up airflow-init
```

This will create the Airflow image, pull the necessary Docker images, create the database, and set up a user for the Airflow webserver.

Then, run the command:

```bash
docker-compose up
```

Wait a few seconds, then go to the local webserver at http://localhost:8080/ and log in with the following credentials:
- **username:** airflow
- **password:** airflow

If everything went ok, you should see a DAG called `pipeline`.

In a separate terminal window, check if DBT is working correctly by running the following command:

```bash
docker-compose exec airflow-webserver dbt build --project-dir /opt/dbt --profiles-dir /opt/dbt
```

If the command ran successfully, you should see a 'Completed successfully' message in green.

## 4. General advice
This project structure is a suggestion. You're free to edit it to your preferences, although it's recommended that you follow it to avoid reconfiguring the Docker Compose file.

When executing any DBT command, be sure to explicitly specify the Project Directory and Profiles Directory. This ensures you can execute commands from any location inside the Docker instance and will help with triggering DBT through Airflow. Both directories are located at `/opt/dbt` inside the Docker container.

In summary, if you are inside a Docker container run:
```bash
dbt <command> --project-dir /opt/dbt --profiles-dir /opt/dbt
```

If you're not inside the container, run:
```bash
docker-compose exec airflow-webserver dbt <command> --project-dir /opt/dbt --profiles-dir /opt/dbt
```
