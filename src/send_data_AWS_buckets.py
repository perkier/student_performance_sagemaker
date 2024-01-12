import sagemaker
import boto3
import pandas as pd
import os

def initiate_aws_instances(region_name='eu-west-2', bucket_name='dockertesting3'):
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


def read_dataset(file_path):
    """
    Reads the dataset from the specified file path.

    Parameters:
    - file_path (str): Path to the dataset file.

    Returns:
    - df (pd.DataFrame): DataFrame containing the dataset.
    """
    df = pd.read_csv(file_path)
    return df


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

# Main script
if __name__ == "__main__":

    bucket_name = "studentperformanceindicator"

    # Initiate AWS instances
    sm_boto3, sess, bucket = initiate_aws_instances(bucket_name = "studentperformanceindicator")
    sk_prefix = "data"

    data_path = os.path.join('..', 'data', "stud.csv")

    # Upload data to S3
    s3_train_path = upload_data_to_s3(sess, data_path, bucket, sk_prefix)

    print(s3_train_path)
