# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the project files into the container
COPY . .

# Install dependencies
RUN pip install -r requirements.txt

# Define the command to run the application
CMD ["python", "app.py"]
