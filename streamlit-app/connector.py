import boto3
import pandas as pd
from typing import List, Optional
import os
from dotenv import load_dotenv
import s3fs

load_dotenv()

class S3Connector:    
    def __init__(self):
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region_name = os.getenv('AWS_REGION')
        
        # Initialize both boto3 and s3fs clients
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )
        
        # Configure S3FileSystem with correct parameters
        self.fs = s3fs.S3FileSystem(
            key=self.aws_access_key_id,
            secret=self.aws_secret_access_key,
            client_kwargs={'region_name': self.region_name}
        )
    
    def list_parquet_files(self, bucket_name: str, prefix: str = '') -> List[str]:
        """List all parquet files in the specified S3 bucket and prefix."""
        try:
            # Use s3fs to list files
            path = f"{bucket_name}/{prefix}*.parquet"
            files = self.fs.glob(path)
            return [f.split(f"{bucket_name}/")[1] for f in files]
        except Exception as e:
            print(f"Error listing files: {str(e)}")
            return []
    
    def read_parquet_file(self, bucket_name: str, file_key: str) -> Optional[pd.DataFrame]:
        """Read a parquet file from S3 and return it as a pandas DataFrame."""
        try:
            path = f"{bucket_name}/{file_key}"
            with self.fs.open(path) as f:
                df = pd.read_parquet(f)
            return df
        except Exception as e:
            print(f"Error reading file {file_key}: {str(e)}")
            return None
    
    def get_all_data(self, bucket_name: str, prefix: str = '') -> Optional[pd.DataFrame]:
        """Read all parquet files in the bucket/prefix and combine them into a single DataFrame."""
        try:
            path = f"{bucket_name}/{prefix}"
            if prefix and not prefix.endswith('/'):
                path = f"{path}/"
            path = f"{path}*.parquet"
            
            # Read all parquet files at once using s3fs
            df = pd.read_parquet(f"s3://{path}", filesystem=self.fs)
            return df
            
        except Exception as e:
            print(f"Error reading data: {str(e)}")
            # Fall back to reading files individually if bulk read fails
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
