import os
import sys
from src.exception import CustomException
from src.logger import logging
import pandas as pd

from sklearn.model_selection import train_test_split
from dataclasses import dataclass

from src.utils import initiate_aws_instances, create_experiment, get_data_from_s3


@dataclass
class DataIngestionConfig:
    train_data_path: str = os.path.join('..','..', 'artifacts', "train.csv")
    test_data_path: str = os.path.join('..', '..','artifacts', "test.csv")
    raw_data_path: str = os.path.join('..', '..', 'artifacts', "data.csv")


class DataIngestion:

    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):

        logging.info("Entered the data ingestion method or component")

        try:

            # Get the directory of the current script
            script_dir = os.path.dirname(os.path.abspath(__file__))

            file_path_relative = os.path.join('..','..', 'data', 'stud.csv')

            # Get the absolute path using the relative path
            file_path = os.path.abspath(os.path.join(script_dir, file_path_relative))

            df = pd.read_csv(file_path)
            logging.info('Read the dataset as dataframe')

            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path), exist_ok=True)

            df.to_csv(self.ingestion_config.raw_data_path, index=False, header=True)

            logging.info("Train test split initiated")
            train_set, test_set = train_test_split(df, test_size=0.2, random_state=42)

            train_set.to_csv(self.ingestion_config.train_data_path, index=False, header=True)

            test_set.to_csv(self.ingestion_config.test_data_path, index=False, header=True)

            logging.info("Ingestion of the data is completed")

            return (
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path)

        except Exception as e:

            logging.info("Failed in data ingestion")
            raise CustomException(e, sys)


if __name__ == "__main__":

    sm_boto3, sess, bucket = initiate_aws_instances()
    experiment, trial = create_experiment("prepare", "data processing", sm_boto3)

    experiment_config_prepare = {"ExperimentName": experiment.experiment_name,
                                 "TrialName": trial.trial_name,
                                 "TrialComponentDisplayName": "prepare"}

    # Get data
    bucket_name = "studentperformanceindicator"
    key = 'data/stud.csv'

    data = get_data_from_s3(bucket_name, key)