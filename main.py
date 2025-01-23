import configparser
import json
import os
from src import ebs_utils
from src import s3_utils

SHUTDOWN_ENABLED = False # For testing purposes

_bsh = ebs_utils.Block_Storage_Handler()

def trigger_shutdown():

    _bsh.remove_block()

    if SHUTDOWN_ENABLED:
        pass
    else:
        exit()

if __name__ == "__main__":

    # load config

    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Read the configuration file
    config.read('details.conf')

    # Accessing sections and options
    # buckets = json.loads(config['S3']['buckets_to_check'].replace('\n', ''))
    buckets = json.loads(config.get("S3","buckets_to_check"))

    conversions = dict(config['Conversions'])

    block_size = int(config['Options']['block_size'])

    print(f'Operating on buckets: {", ".join(buckets)}')

    convertible_formats = list(conversions.keys())

    print(f'Selected for conversion: {", ".join(convertible_formats)}')

    # Get list of all files which can be converted
    
    s3_manager = s3_utils.S3_Wrapper()

    target_files = {}

    found_files = False

    print('Scanning all buckets...')

    for bucket in buckets:

        print(f'Scanning bucket: {bucket}')

        files_in_bucket = s3_manager.get_files_by_extension(bucket, '', convertible_formats)

        found_files = found_files or len(files_in_bucket)

        target_files[bucket] = files_in_bucket

    if not found_files:
        # No files found! Trigger an immediate shutdown

        trigger_shutdown()

    # We have files to convert. Attatch the ebs block

    output_path = _bsh.add_and_attach(block_size)

    for bucket in buckets:

        for target_filepath in target_files[bucket]:

            print(f'Downloading: {target_filepath}')

            file_name = os.path.basename(target_filepath)

            download_to = os.path.join(output_path, file_name)

            s3_manager.download_file(bucket, target_filepath, download_to)

            exit()