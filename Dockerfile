FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy requirements (if any) and install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script and any related files
COPY logs-forwarder.py /app/logs-forwarder.py

# Run the script
CMD ["python", "logs-forwarder.py"]