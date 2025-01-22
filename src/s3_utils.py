import boto3
import os

class S3_Wrapper:
    def __init__(self):
        self.s3_client = boto3.client('s3')

    def get_files_by_extension(self,bucket_name, folder_path, file_extensions):
        """
        Scans an S3 folder and returns all files with specified extensions.

        Args:
            bucket_name (str): Name of the S3 bucket.
            folder_path (str): Path to the folder in the S3 bucket.
            file_extensions (list): List of file extensions to look for (e.g., [".txt", ".jpg"]).

        Returns:
            list: A list of file keys matching the specified extensions.
        """
        matching_files = []

        if not folder_path.endswith('/'):
            folder_path += '/'
        
        while folder_path and folder_path[0] == '/':
            folder_path = folder_path[1:]

        paginator = self.s3_client.get_paginator('list_objects_v2')
        operation_parameters = {
            'Bucket': bucket_name,
            'Prefix': folder_path
        }

        for page in paginator.paginate(**operation_parameters):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if any(key.endswith(ext) for ext in file_extensions):
                        matching_files.append(key)

        return matching_files

    def download_file(self, bucket_name, file_key, download_path):
        """
        Downloads a specified file from an S3 bucket to a specific location on disk.

        Args:
            bucket_name (str): Name of the S3 bucket.
            file_key (str): Key of the file in the S3 bucket.
            download_path (str): Local file path to download the file to.

        Returns:
            None
        """

        # Ensure the directory exists
        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        # Download the file
        self.s3_client.download_file(bucket_name, file_key, download_path)