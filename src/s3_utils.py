import boto3

def get_files_by_extension(bucket_name, folder_path, file_extensions):
    """
    Scans an S3 folder and returns all files with specified extensions.

    Args:
        bucket_name (str): Name of the S3 bucket.
        folder_path (str): Path to the folder in the S3 bucket.
        file_extensions (list): List of file extensions to look for (e.g., [".txt", ".jpg"]).

    Returns:
        list: A list of file keys matching the specified extensions.
    """
    s3_client = boto3.client('s3')
    matching_files = []

    if not folder_path.endswith('/'):
        folder_path += '/'
    
    while folder_path and folder_path[0] == '/':
        folder_path = folder_path[1:]

    paginator = s3_client.get_paginator('list_objects_v2')
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