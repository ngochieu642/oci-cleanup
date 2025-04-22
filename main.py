import json
import argparse
import time
import oci
from tenacity import retry, stop_after_attempt, wait_fixed
from tqdm import tqdm


def list_object_versions(object_storage_client, bucket_name, namespace):
    """List all object versions in a bucket"""
    try:
        response = object_storage_client.list_object_versions(
            namespace_name=namespace,
            bucket_name=bucket_name
        )
        
        # Convert ObjectVersionCollection to list using the items property
        all_objects = []
        for item in response.data.items:
            all_objects.append(item)
            
        return all_objects
    except Exception as e:
        print(f"Error listing object versions: {e}")
        return []


@retry(stop=stop_after_attempt(4), wait=wait_fixed(10))
def delete_object_with_retry(object_storage_client, namespace, bucket_name, object_name, version_id):
    """Delete an object version with retry mechanism"""
    return object_storage_client.delete_object(
        namespace_name=namespace,
        bucket_name=bucket_name,
        object_name=object_name,
        version_id=version_id
    )


@retry(stop=stop_after_attempt(4), wait=wait_fixed(10))
def delete_bucket_with_retry(object_storage_client, namespace, bucket_name):
    """Delete a bucket with retry mechanism"""
    try:
        object_storage_client.delete_bucket(
            namespace_name=namespace,
            bucket_name=bucket_name
        )
        print(f"\nBucket '{bucket_name}' deleted successfully")
        return True
    except oci.exceptions.ServiceError as e:
        if e.code == "BucketNotEmpty":
            print(f"\nBucket '{bucket_name}' is not empty. Please ensure all objects are deleted first.")
        else:
            print(f"\nError deleting bucket '{bucket_name}': {e}")
        return False


def clean_up_bucket(oci_profile, bucket_name, namespace, max_retries=4, retry_delay=10):
    """Delete all objects from a bucket and then delete the bucket itself"""
    # Load OCI config from specified profile
    config = oci.config.from_file(profile_name=oci_profile)
    object_storage_client = oci.object_storage.ObjectStorageClient(config)

    # Get list of object versions
    print("Fetching object versions...")
    objects = list_object_versions(object_storage_client, bucket_name, namespace)
    
    if not objects:
        print("No objects found in the bucket.")
    else:
        # Create progress bar for object deletion
        with tqdm(total=len(objects), desc="Deleting objects") as pbar:
            for item in objects:
                object_name = item.name
                version_id = item.version_id

                try:
                    delete_object_with_retry(
                        object_storage_client,
                        namespace,
                        bucket_name,
                        object_name,
                        version_id
                    )
                    pbar.set_description(f"Deleted: {object_name}")
                except Exception as e:
                    print(f"\nError deleting {object_name} | Version ID: {version_id}: {e}")
                
                pbar.update(1)
    
    # After all objects are deleted, delete the bucket
    print("\nAttempting to delete the bucket...")
    delete_bucket_with_retry(object_storage_client, namespace, bucket_name)


def main():
    # Set up CLI argument parsing
    parser = argparse.ArgumentParser(description="Clean up an OCI bucket by deleting all objects and the bucket itself")
    parser.add_argument("--oci_profile", required=True, help="OCI profile to use from the config file")
    parser.add_argument("--bucket_name", required=True, help="The name of the bucket to clean up")
    parser.add_argument("--max_retries", type=int, default=4, help="Maximum number of retry attempts")
    parser.add_argument("--retry_delay", type=int, default=10, help="Delay between retries in seconds")

    # Parse arguments
    args = parser.parse_args()

    # Call the clean_up_bucket function with parsed arguments
    clean_up_bucket(
        args.oci_profile,
        args.bucket_name,
        'lrbvkel2wjot',
        max_retries=args.max_retries,
        retry_delay=args.retry_delay
    )


if __name__ == "__main__":
    main()
