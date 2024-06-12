# Use the official Jupyter Notebook image as the base image
FROM jupyter/scipy-notebook

# Switch to root user to install additional packages
USER root

# Install Java (OpenJDK 17) for PySpark
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget gnupg2 lsb-release

# Install PostgreSQL client (psql) and pgAdmin
RUN apt-get install -y postgresql-client && \
    wget --quiet -O - https://www.pgadmin.org/static/packages_pgadmin_org.pub | apt-key add - && \
    echo "deb https://ftp.postgresql.org/pub/pgadmin/pgadmin4/apt/$(lsb_release -cs) pgadmin4 main" | tee /etc/apt/sources.list.d/pgadmin4.list && \
    apt-get update && \
    apt-get install -y pgadmin4

# Download PostgreSQL JDBC driver
RUN wget -P /usr/local/share/postgresql/ https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Install Apache Spark
RUN wget -q -O /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Switch back to jovyan user for running Jupyter
USER jovyan

# Install additional Python packages
RUN pip install \
    pyspark \
    joblib \
    fuzzywuzzy \
    sqlalchemy \
    psycopg2-binary

# Copy the notebook files into the container
COPY . /home/nimbly-dev/work

# Set the working directory
WORKDIR /home/nimbly-dev/work

# Expose the Jupyter Notebook port
EXPOSE 8888

# Start Jupyter Notebook
CMD ["start-notebook.sh", "--NotebookApp.token=''"]
