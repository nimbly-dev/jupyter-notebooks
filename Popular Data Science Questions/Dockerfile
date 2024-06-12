# Use the official Jupyter Notebook image as the base image
FROM jupyter/scipy-notebook

# Install additional packages
RUN pip install \
    pandas \
    numpy \
    plotly \
    matplotlib \
    mlxtend

# Copy the notebook files into the container
COPY . /home/nimbly-dev/work

# Set the working directory
WORKDIR /home/nimbly-dev/work

# Expose the Jupyter Notebook port
EXPOSE 8888

# Start Jupyter Notebook
CMD ["start-notebook.sh", "--NotebookApp.token=''"]
