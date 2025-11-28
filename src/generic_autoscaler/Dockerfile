# Use a slim Python image for efficiency
FROM python:3.11-slim

# Set the working directory
WORKDIR /usr/src/app

# Install necessary libraries (kubernetes client and others used in controller.py)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the controller code
COPY controller.py .

# Command to run the controller
CMD ["python3", "controller.py"]