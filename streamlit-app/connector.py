import boto3
import pandas as pd
from typing import List, Optional
import os
from dotenv import load_dotenv

load_dotenv()

class S3Connector:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
    
    def list_parquet_files(self, bucket_name: str, prefix: str = '') -> List[str]:
        """List all parquet files in the specified S3 bucket and prefix."""
        response = self.s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        parquet_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.parquet'):
                    parquet_files.append(obj['Key'])
        
        return parquet_files
    
    def read_parquet_file(self, bucket_name: str, file_key: str) -> Optional[pd.DataFrame]:
        """Read a parquet file from S3 and return it as a pandas DataFrame."""
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            df = pd.read_parquet(response['Body'])
            return df
        except Exception as e:
            print(f"Error reading file {file_key}: {str(e)}")
            return None
    
    def get_all_data(self, bucket_name: str, prefix: str = '') -> Optional[pd.DataFrame]:
        """Read all parquet files in the bucket/prefix and combine them into a single DataFrame."""
        parquet_files = self.list_parquet_files(bucket_name, prefix)
        
        if not parquet_files:
            print("No parquet files found in the specified location")
            return None
        
        dfs = []
        for file_key in parquet_files:
            df = self.read_parquet_file(bucket_name, file_key)
            if df is not None:
                dfs.append(df)
        
        if not dfs:
            return None
        
        return pd.concat(dfs, ignore_index=True)
