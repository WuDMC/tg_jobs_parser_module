# Use the official Python 3.10-slim image as a base
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the local directory contents into the container
COPY . /app/jobs_parser

# Install pip and dependencies
RUN pip install --upgrade pip setuptools wheel

# If you have a requirements.txt, install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

# Install the package
RUN python jobs_parser/setup.py install

# Default command to run in the container
CMD ["bash"]
