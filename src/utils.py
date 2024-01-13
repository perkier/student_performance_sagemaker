import os
import sys

import pickle

from src.exception import CustomException

import sagemaker
import boto3
from smexperiments.experiment import Experiment
from smexperiments.trial import Trial
from sagemaker.workflow.parameters import (ParameterInteger, ParameterString, ParameterFloat)

import time


def create_experiment(pipeline_name, description, sm):

    timestamp = int(time.time())

    pipeline_name = f"{pipeline_name}-{timestamp}"

    experiment = Experiment.create(experiment_name=pipeline_name,
                                   description=description,
                                   sagemaker_boto_client=sm)

    trial = Trial.create(trial_name= f"trial-{pipeline_name}-{timestamp}",
                         experiment_name= experiment.experiment_name,
                         sagemaker_boto_client=sm)

    return experiment, trial


def initiate_aws_instances(region_name='eu-west-2', bucket_name='studentperformanceindicator'):

    """
    Initiates AWS SageMaker instances and session.

    Parameters:
    - region_name (str): AWS region name.
    - bucket_name (str): Name of the S3 bucket.

    Returns:
    - sm_boto3 (boto3.client): SageMaker Boto3 client.
    - sess (sagemaker.Session): SageMaker session.
    """
    sm_boto3 = boto3.client("sagemaker", region_name=region_name)
    sess = sagemaker.Session()
    bucket = bucket_name

    print(f"Using bucket {bucket}")

    return sm_boto3, sess, bucket


def upload_data_to_s3(session, local_path, bucket, key_prefix):
    """
    Uploads data to Amazon S3.

    Parameters:
    - session (sagemaker.Session): SageMaker session.
    - local_path (str): Local path to the data file.
    - bucket (str): Name of the S3 bucket.
    - key_prefix (str): Key prefix for the S3 object.

    Returns:
    - s3_path (str): S3 path where the data is uploaded.
    """
    s3_path = session.upload_data(path=local_path, bucket=bucket, key_prefix=key_prefix)

    return s3_path


def get_data_from_s3(bucket_name, key):
    """
    Get data from an S3 bucket.

    Parameters:
    - bucket_name: The name of the S3 bucket.
    - key: The object key (path) of the file you want to retrieve.

    Returns:
    - The content of the file as a string.
    """
    # Create an S3 client
    s3 = boto3.client('s3')

    try:
        # Get the object from S3
        response = s3.get_object(Bucket=bucket_name, Key=key)

        # Read the content of the object
        data = response['Body'].read().decode('utf-8')

        return data
    except Exception as e:
        print(f"Error: {e}")
        return None


def save_object(file_path, obj):

    try:

        dir_path = os.path.dirname(file_path)

        os.makedirs(dir_path, exist_ok=True)

        with open(file_path, "wb") as file_obj:
            pickle.dump(obj, file_obj)

    except Exception as e:
        raise CustomException(e, sys)


def load_object(file_path):
    try:
        with open(file_path, "rb") as file_obj:
            return pickle.load(file_obj)

    except Exception as e:
        raise CustomException(e, sys)