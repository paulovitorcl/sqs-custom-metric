import logging
import boto3 
from botocore.exceptions import ClientError
import json

AWS_REGION = 'sa-east-1'

#logger config
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')

#session config
session = boto3.Session(profile_name='desenvolvimento')
sqs_client = session.client("sqs", region_name=AWS_REGION)
ecs_client = session.client("ecs", region_name=AWS_REGION)
cloudwatch_client = session.client("cloudwatch", region_name=AWS_REGION)


def get_tasks_running(cluster_name, service_name):
    """
    Gets the number of active task in service
    """
    try:
        response = ecs_client.describe_services(cluster=cluster_name, services=[service_name])
    except ClientError:
        logger.exception(f'Could not get tasks in service - {service_name}.')
        raise
    else: 
        return response
    
def get_queue_attributes(queue_url, attribute_names):
    """
    Gets attributes for the specified queued.
    """
    try: 
        response = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=attribute_names)
    except ClientError: 
        logger.exception(f'Could not get queue attributes - {queue_url}.')
        raise
    else: 
        return response

def put_metric_data(approximateNumberOfMessages, numberOfActiveTaskInService, sqs_name, service_name):
    """
    Gets and put the metric backlog per task.
    """
    try:
        backlogPerTask = approximateNumberOfMessages/numberOfActiveTaskInService
        response = cloudwatch_client.put_metric_data(
            Namespace = 'Messages/Tasks',
            MetricData=[
                {
                    'MetricName': 'Backlog per task',
                    'Dimensions': [
                        {
                            'Name': 'SQS Queue',
                            'Value': sqs_name
                        }, {
                            'Name': 'ECS Service',
                            'Value': service_name
                        }
                    ],
                    'Value': backlogPerTask,
                    'Unit':'Count'
                },
            ]
        )
    except ClientError:
        logger.exception(f'Could not calculate the metric - backlogPerTask.')
        raise
    else:
        return response
    



if __name__ == '__main__':
    #CONSTANTS
    QUEUE_URL = '<your_queue_url>'
    ATTRIBUTE_NAMES = ['ApproximateNumberOfMessages']

    CLUSTER_NAME = '<your_cluster_name>'
    SERVICE_NAME = '<your_service_name>'

    SQS_NAME = '<your_sqs_queue_name>'


    #FUNCTIONS
    attributes = int(get_queue_attributes(QUEUE_URL, ATTRIBUTE_NAMES)['Attributes']['ApproximateNumberOfMessages'])
    tasks = get_tasks_running(CLUSTER_NAME, SERVICE_NAME)['services'][0]['runningCount']
    metric = put_metric_data(attributes, tasks, SQS_NAME, SERVICE_NAME)


    #LOGS
    logger.info(f'Queue attributes:\n{json.dumps(attributes, indent=4)}')
    logger.info(f'Tasks running:\n{json.dumps(tasks, indent=4)}')
    logger.info(f'Backlog per task:\n{json.dumps(metric, indent=4)}')
    

    
