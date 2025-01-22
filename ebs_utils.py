import boto3
import time
import os
import subprocess
import configparser
from typing import List
import os

_CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'details.conf')

class Block_Storage_Handler:
    '''
    Handle the mounting,unmounting,provisioning and deletion of an EBS volume

    Currently only supports the handling of one block at a time. 

    Don't try and instance multiple versions of the handler either, limitation is mount structure

    Also for simplicity, this should be run on the target ec2 instance. It will break otherwise. (And possibly mess up your machines file system :>)
    '''

    def __init__(self):
        # Load params from the conf file
        conf = configparser.ConfigParser()

        if not os.path.exists(_CONFIG_FILE):
            raise FileNotFoundError(f'Configuration file not found! {_CONFIG_FILE}')

        conf.read(_CONFIG_FILE)

        self.instance_id = conf['ec2']['instance_id']
        self.zone = conf['ec2']['zone']
        self.tag = conf['ec2']['ebs_tag']
        self.tag_key = "Purpose"

        self.volume_id = None # Track the attached block ID 
    
        # Initialize the ec2 client
        self.ec2 = boto3.client('ec2')

        # Check that no blocks have been instanced with our reserved tag.
        # If these blocks exist, we should remove them asap. 

        self.remove_block()

    def remove_block(self):
        response = self.ec2.describe_volumes(
            Filters=self.get_filter()
        )

        volumes = response['Volumes']

        if len(volumes) == 0:
            # Check and remove mount point even if no volumes are found
            self.cleanup_mount_point()
            return

        print(f"Detected {len(volumes)} old volumes")
        print('Cleaning up old EBS Volumes...')

        # Step 2: Iterate over volumes and check their attachment status
        for volume in volumes:
            volume_id = volume['VolumeId']
            state = volume['State']
            attachments = volume['Attachments']

            print(f"Processing volume: {volume_id}, State: {state}")

            # If the volume is attached, detach it first
            if attachments:
                for attachment in attachments:
                    instance_id = attachment['InstanceId']
                    print(f"Volume {volume_id} is attached to instance {instance_id}, detaching...")

                    # Step 3: Detach the volume
                    self.ec2.detach_volume(VolumeId=volume_id)
                    print(f"Detaching volume {volume_id} from instance {instance_id}")

                    # Wait until the volume is detached
                    self.ec2.get_waiter('volume_available').wait(VolumeIds=[volume_id])
                    print(f"Volume {volume_id} detached successfully")

            # Step 4: Delete the volume
            self.ec2.delete_volume(VolumeId=volume_id)
            print(f"Volume {volume_id} deleted")
        
        # Clean up the mount point after volumes have been detached and deleted
        self.cleanup_mount_point()

    def cleanup_mount_point(self):
        """
        Unmounts and removes the mount point directory if it exists.
        """
        mount_point = '/mnt/ebs-volume'
        if os.path.exists(mount_point):
            print('Cleaning up old mount point...')

            if os.path.ismount(mount_point):
                print(f"Unmounting {mount_point}...")
                subprocess.run(['sudo', 'umount', mount_point], check=True)
            print(f"Removing directory {mount_point}...")
            subprocess.run(['sudo', 'rm', '-rf', mount_point], check=True)

    def get_tag(self) -> List:
        '''
        Return a list object that can be used directly as a tag in boto3 ec2 calls
        '''

        return [
                {
                'Key': self.tag_key,
                'Value': self.tag
                }
            ]
    
    def get_filter(self):
        '''
        Return a list object that can be used directly as a tag in boto3 ec2 calls
        '''
        return [
            {
                'Name': 'tag:' + self.tag_key,
                'Values': [self.tag]
            }
        ]

    def add_and_attach(self, size_gb, volume_type='gp2'):
        # Create a new EBS volume
        response = self.ec2.create_volume(
            Size=size_gb,
            AvailabilityZone=self.zone,
            VolumeType=volume_type
        )

        self.volume_id = response['VolumeId']
        print(f"Created volume: {self.volume_id}")

        # Attach some tags so we can find this volume later
        self.ec2.create_tags(
            Resources=[self.volume_id],
            Tags=self.get_tag()
        )

        print(f"Tag {self.tag_key}:{self.tag} attached to volume {self.volume_id}")

        # Wait for the volume to become available
        print('Waiting for volume to be available...')
        self.ec2.get_waiter('volume_available').wait(VolumeIds=[self.volume_id])

        print('Attaching volume...')
        self.ec2.attach_volume(
            VolumeId=self.volume_id,
            InstanceId=self.instance_id,
            Device='/dev/sdf'
        )

        # Wait for the device to appear on the instance
        print('Waiting for device to be recognized by the OS...')
        device_path = '/dev/nvme1n1'
        timeout = 60  # seconds
        elapsed = 0
        while not os.path.exists(device_path) and elapsed < timeout:
            time.sleep(1)
            elapsed += 1

        if not os.path.exists(device_path):
            raise TimeoutError(f"Device {device_path} did not appear within {timeout} seconds")

        print('Mounting...')
        # Format the volume (assumes this is a new volume)
        subprocess.run(['sudo', 'mkfs', '-t', 'ext4', device_path], check=True)

        # Mount the volume
        subprocess.run(['sudo', 'mkdir', '-p', '/mnt/ebs-volume'], check=True)
        subprocess.run(['sudo', 'mount', device_path, '/mnt/ebs-volume'], check=True)

        # Adjust ownership and permissions
        username = os.getlogin()
        groupname = username
        
        print('Adjusting permissions on the mounted volume...')
        subprocess.run(['sudo', 'chown', f'{username}:{groupname}', '/mnt/ebs-volume'], check=True)
        subprocess.run(['sudo', 'chmod', '770', '/mnt/ebs-volume'], check=True)

        print('Done!')
        print('EBS block mounted to location: /mnt/ebs-volume')

        return '/mnt/ebs-volume'