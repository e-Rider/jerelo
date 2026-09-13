# Use the official lightweight Python 3.12 base image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies without caching to keep the image size minimal
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main extraction script into the container
COPY extract_prozorro.py .

# Define the command to run the application when the container starts
CMD ["python", "extract_prozorro.py"]