import boto3
import csv
import json
import time
from typing import Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KinesisCSVWriter:
    def __init__(self, stream_name: str, region_name: str = 'eu-west-1'):
        """
        Initialize the Kinesis CSV Writer
        
        Args:
            stream_name (str): Name of the Kinesis stream
            region_name (str): AWS region name
        """
        self.stream_name = stream_name
        self.kinesis_client = boto3.client('kinesis', region_name=region_name)
        
    def write_csv_to_kinesis(self, csv_file_path: str, partition_key_column: str = None) -> None:
        """
        Read CSV file and write records to Kinesis
        
        Args:
            csv_file_path (str): Path to the CSV file
            partition_key_column (str): Column name to use as partition key. If None, uses row number
        """
        try:
            with open(csv_file_path, 'r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                
                for row_number, row in enumerate(csv_reader, 1):
                    # Convert row to JSON string
                    data = json.dumps(row)
                    
                    # Use specified column as partition key or row number as fallback
                    partition_key = str(row.get(partition_key_column, row_number))
                    
                    try:
                        response = self.kinesis_client.put_record(
                            StreamName=self.stream_name,
                            Data=data,
                            PartitionKey=partition_key
                        )
                        logger.info(f"Successfully wrote record {row_number} to Kinesis. ShardId: {response['ShardId']}")
                        
                        # Add a small delay to avoid throttling
                        time.sleep(0.1)
                        
                    except Exception as e:
                        logger.error(f"Error writing record {row_number} to Kinesis: {str(e)}")
                        raise
                        
        except Exception as e:
            logger.error(f"Error processing CSV file: {str(e)}")
            raise

def main():
    # Example usage
    stream_name = "telcopulse-stream"  
    csv_file_path = "Data/mobile-logs.csv"   
    
    try:
        writer = KinesisCSVWriter(stream_name)
        writer.write_csv_to_kinesis(csv_file_path)  
        logger.info("Successfully processed all records")
        
    except Exception as e:
        logger.error(f"Failed to process CSV file: {str(e)}")

if __name__ == "__main__":
    main()
