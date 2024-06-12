# Building Fast Queries on a Csv

The original goal of the project is to build fast and efficient code for querying in a .csv file. This was expanded to use an ETL script that can load the data on a Postgres DB, use PySpark (In Memory), and use levenstein algorithm to clean the data. The main idea here is to leverage distributed computing and how can it make the process of large datasets much faster.

The project is a bit of verbose as the style of writing is of a tone of discover and extensive documentation on why and how the code was written. The docker compose also contains a PgAdmin instances that can be accessed on localhost:8080.


## To run locally

1. The recommended way to run is building the docker image and running it. Please download [Docker Desktop]('https://www.docker.com/products/docker-desktop/') if it is not installed on your local environment. 
2. Run command: `docker-compose up --build`

## Stopping the Docker instances and cleanup

1. On the command-line where the docker-image is running, on keyboard CTRL-F to stop the docker instance
2. `docker-compose down` to remove the docker contaienr
3. To remove the image run: `docker rmi building-fast-queries-on-csv`


## Issues found:

1. Ensure that localhost:5432 is not being used, if so you can either change the port on the docker-compose or kill the process:
    - Run `netstat -aon | findstr :5432` on windows to check if port is in use
    - Run `taskkill /PID {PID} /F` to kill the process