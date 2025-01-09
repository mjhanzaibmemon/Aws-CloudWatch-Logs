from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, NotFoundError
import boto3
import time
import socket
import uuid
from botocore.exceptions import ClientError
import json
#from dotenv import load_dotenv
import os
from pathlib import Path
import logging
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)

#load_dotenv()
# logger = logging.getLogger()


# print(f"AWS Access Key: {os.getenv('AWS_ACCESS_KEY_ID')}")
# print(f"AWS Secret Key: {os.getenv('AWS_SECRET_ACCESS_KEY')}")
# print(f"AWS Region: {os.getenv('AWS_REGION')}")

aws_region = os.getenv("AWS_REGION", "us-east-2")
cloudwatch_client = boto3.client('logs', region_name=aws_region)


es_client = Elasticsearch("http://3.132.58.136:9200/")

# Configuration for log groups
LOG_GROUPS = [
    {
        "log_group_name": "/ecs/proxy-manager",
        "app_name": "proxy-manager"
    },
    {
        "log_group_name": "/ecs/scrapper-service-task-definition",
        "app_name": "scrapper-service-new"
    },
    {
        "log_group_name": "/ecs/event-stats-service",
        "app_name": "event-stats-service"
    },
    {
        "log_group_name": "/ecs/proxy-manager-redis",
        "app_name": "proxy-manager-redis-service"
    },
    {
        "log_group_name": "/ecs/ticketparser-redis",
        "app_name": "ticketparser-redis-service"
    },
    {
        "log_group_name": "/ecs/listing-data-storage-api-dedicated",
        "app_name": "listing-data-storage-api-dedicated"
    },
    {
        "log_group_name": "/ecs/venue-venture-api",
        "app_name": "venue-venture-api"
    },
    {
        "log_group_name": "/ecs/events-api-task-definition",
        "app_name": "events-api-service"
    },
    {
        "log_group_name": "/ecs/legacy-provider-api",
        "app_name": "legacy-provider-api"
    },
    {
        "log_group_name": "/ecs/scraper-storage",
        "app_name": "scraper-storage-service"
    },
    {
        "log_group_name": "/ecs/ticketparser-selenium",
        "app_name": "ticketparser-selenium-chrome"
    },
    {
        "log_group_name": "/ecs/events_stats_service/mongodb",
        "app_name": "event-stats-mongo-service"
    },
    {
        "log_group_name": "/ecs/app-sokratiq-task-definition",
        "app_name": "app-sokratiq-ui"
    },
    {
        "log_group_name": "/ecs/scrapper-service-v2",
        "app_name": "scrapper-service-v2"
    },
    {
        "log_group_name": "/ecs/app-sokratiq-staging-api",
        "app_name": "app-sokratiq-staging-api"
    },
    {
        "log_group_name": "/ecs/listing-data-storage-api",
        "app_name": "listing-data-storage-api"
    },
    {
        "log_group_name": "/ecs/cookie-manager",
        "app_name": "cookie-manager"
    },
    {
        "log_group_name": "/ecs/trigger-service-logger-api-task-def",
        "app_name": "trigger-service-logger-api-service"
    },
    {
        "log_group_name": "/ecs/skybox-sync-task-def",
        "app_name": "skybox-sync-service"
    },
    {
        "log_group_name": "/ecs/app-sokratiq-staging",
        "app_name": "app-sokratiq-staging"
    },
    {
        "log_group_name": "/ecs/trigger-service",
        "app_name": "trigger-service"
    },
    {
        "log_group_name": "/ecs/app-sokratiq-api",
        "app_name": "app-sokratiq-api"
    },
    {
        "log_group_name": "/ecs/event-processor",
        "app_name": "event-data-processor"
    },
    {
        "log_group_name": "/ecs/drop-checker--fargate-task-definition",
        "app_name": "drop-checker-service"
    },
    {
        "log_group_name": "/ecs/prometheus",
        "app_name": "prometheus-service"
    }
]

# Rate limiting configuration
RATE_LIMIT_DELAY = 0.5  # 500ms between API calls
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

# Add checkpoint configuration
CHECKPOINT_DIR = "checkpoints"
Path(CHECKPOINT_DIR).mkdir(exist_ok=True)

def get_checkpoint_file(log_group_name):
    """Get checkpoint file path for a log group"""
    safe_name = log_group_name.replace('/', '_').lstrip('_')
    return os.path.join(CHECKPOINT_DIR, f"{safe_name}_checkpoint.json")

def save_checkpoint(log_group_name, timestamp):
    """Save the last processed timestamp"""
    checkpoint_file = get_checkpoint_file(log_group_name)
    with open(checkpoint_file, 'w') as f:
        json.dump({'last_timestamp': timestamp}, f)

def load_checkpoint(log_group_name):
    """Load the last processed timestamp"""
    checkpoint_file = get_checkpoint_file(log_group_name)
    try:
        with open(checkpoint_file, 'r') as f:
            data = json.load(f)
            return datetime.fromtimestamp(data['last_timestamp'] / 1000)
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        # Return a timestamp from 5 minutes ago if no checkpoint exists
        return datetime.utcnow() - timedelta(minutes=5)

def get_logs_with_retry(log_group_name, start_time, end_time):
    for attempt in range(MAX_RETRIES):
        try:
            response = cloudwatch_client.filter_log_events(
                logGroupName=log_group_name,
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000)
            )
            return response['events']
        except ClientError as e:
            if e.response['Error']['Code'] == 'ThrottlingException':
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
                    continue
            raise
        finally:
            time.sleep(RATE_LIMIT_DELAY)  # Rate limiting between API calls
    return []


def create_index_if_not_exists(index_name):
    try:
        es_client.indices.get(index=index_name)
    except NotFoundError:
        mapping = {
            "mappings": {
                "properties": {
                    "@timestamp": {"type": "date"},
                    "message": {"type": "text"},
                    "log.file.path": {"type": "keyword"},
                    "environment": {"type": "keyword"},
                    "app_name": {"type": "keyword"},
                    "awscloudwatch": {
                        "properties": {
                            "log_group": {"type": "keyword"},
                            "log_stream": {"type": "keyword"},
                            "ingestion_time": {"type": "date"}
                        }
                    }
                }
            }
        }
        es_client.indices.create(index=index_name, body=mapping)


def enrich_log_data(log, log_group_config):
    # Convert timestamp from milliseconds to ISO format
    log_timestamp = datetime.fromtimestamp(log['timestamp'] / 1000).isoformat() + 'Z'
    current_time = datetime.utcnow().isoformat() + 'Z'

    return {
        "@timestamp": log_timestamp,  # Use the log's actual timestamp
        "message": log['message'],
        "log": {
            "file": {
                "path": log.get('logStream', '')
            }
        },
        "environment": "production",
        "app_name": log_group_config["app_name"],
        "awscloudwatch": {
            "log_group": log_group_config["log_group_name"],
            "log_stream": log.get('logStream', ''),
            "ingestion_time": current_time
        }
    }


def get_index_name(app_name):
    return f"{app_name}--{datetime.utcnow().strftime('%Y.%m.%d')}"


def push_logs_to_elasticsearch(logs, log_group_config):
    index_name = get_index_name(log_group_config["app_name"])
    create_index_if_not_exists(index_name)
    for log in logs:
        enriched_log = enrich_log_data(log, log_group_config)
        es_client.index(index=index_name, document=enriched_log)


def process_log_group(log_group_config, end_time):
    try:
        log_group_name = log_group_config['log_group_name']
        start_time = load_checkpoint(log_group_name)

        print(f"Processing {log_group_name} from {start_time} to {end_time}")

        logs = get_logs_with_retry(
            log_group_name,
            start_time,
            end_time
        )

        if logs:
            push_logs_to_elasticsearch(
                logs,
                log_group_config,
            )

        # Save the last processed timestamp
        save_checkpoint(log_group_name, int(end_time.timestamp() * 1000))
        # Add print statements for log group and number of logs
        print(f"Log group: {log_group_config['log_group_name']}")
        print(f"Fetched logs: {len(logs)}")

    except Exception as e:
        print(f"Error processing log group {log_group_name}: {str(e)}")

def main():
    interval = 60

    while True:
        end_time = datetime.utcnow()

        for log_group_config in LOG_GROUPS:
            process_log_group(log_group_config, end_time)

        time.sleep(interval)

if __name__ == "__main__":
    main()