# Popular Data Science Questions

The project is about gathering posts data that has been extracted from StackExchange API, libraries used are pandas, matplotlib, and Apriori Algorithm. 


## To run locally

1. The recommended way to run is building the docker image and running it. Please download [Docker Desktop]('https://www.docker.com/products/docker-desktop/') if it is not installed on your local environment. 
2. Run command: `docker-compose up --build to build and start server`

## Stopping the Docker instances and cleanup

1. On the command-line where the docker-image is running, on keyboard CTRL-F to stop the docker instance
2. `docker-compose down` to remove the docker contaienr
3. To remove the image run: `docker rmi popular-data-science-questions-notebook`