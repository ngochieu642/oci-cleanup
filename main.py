import argparse

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


def verify_bucket_exists(object_storage_client, namespace, bucket_name):
    """Verify if a bucket exists"""
    try:
        object_storage_client.get_bucket(
            namespace_name=namespace,
            bucket_name=bucket_name
        )
        return True
    except oci.exceptions.ServiceError as e:
        if e.code == "BucketNotFound":
            print(f"\nBucket '{bucket_name}' does not exist.")
        else:
            print(f"\nError verifying bucket '{bucket_name}': {e}")
        return False


def list_preauthenticated_requests(object_storage_client, namespace, bucket_name):
    """List all preauthenticated requests in a bucket"""
    try:
        response = object_storage_client.list_preauthenticated_requests(
            namespace_name=namespace,
            bucket_name=bucket_name,
            limit=1000
        )
        return response.data
    except Exception as e:
        print(f"Error listing preauthenticated requests for bucket '{bucket_name}': {e}")
        return []


@retry(stop=stop_after_attempt(4), wait=wait_fixed(10))
def delete_par_with_retry(object_storage_client, namespace, bucket_name, par_id):
    """Delete a preauthenticated request with retry mechanism"""
    try:
        object_storage_client.delete_preauthenticated_request(
            namespace_name=namespace,
            bucket_name=bucket_name,
            par_id=par_id
        )
        return True
    except Exception as e:
        print(f"\nError deleting preauthenticated request {par_id}: {e}")
        return False


def clean_up_bucket(object_storage_client, bucket_name, namespace, bucket_pbar=None):
    """Delete all objects from a bucket and then delete the bucket itself"""
    bucket_desc = f"Cleaning bucket: {bucket_name}"
    if bucket_pbar:
        bucket_pbar.set_description(bucket_desc)
    else:
        print(f"\n{bucket_desc}")

    # Verify bucket exists
    if not verify_bucket_exists(object_storage_client, namespace, bucket_name):
        if bucket_pbar:
            bucket_pbar.update(1)
        return False

    # Get list of object versions
    objects = list_object_versions(object_storage_client, bucket_name, namespace)

    if not objects:
        print(f"No objects found in bucket '{bucket_name}'")
    else:
        # Create progress bar for object deletion
        with tqdm(total=len(objects), desc=f"Deleting objects in {bucket_name}", leave=False) as obj_pbar:
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
                    obj_pbar.set_description(f"Deleted: {object_name}")
                except Exception as e:
                    print(f"\nError deleting {object_name} | Version ID: {version_id}: {e}")

                obj_pbar.update(1)

    # Delete all preauthenticated requests
    pars = list_preauthenticated_requests(object_storage_client, namespace, bucket_name)
    if pars:
        with tqdm(total=len(pars), desc=f"Deleting preauthenticated requests in {bucket_name}",
                  leave=False) as par_pbar:
            for par in pars:
                try:
                    delete_par_with_retry(
                        object_storage_client,
                        namespace,
                        bucket_name,
                        par.id
                    )
                    par_pbar.set_description(f"Deleted PAR: {par.id}")
                except Exception as e:
                    print(f"\nError deleting PAR {par.id}: {e}")

                par_pbar.update(1)
    else:
        print(f"No preauthenticated requests found in bucket '{bucket_name}'")

    # After all objects and PARs are deleted, delete the bucket
    success = delete_bucket_with_retry(object_storage_client, namespace, bucket_name)

    if bucket_pbar:
        bucket_pbar.update(1)

    return success


def clean_up_buckets_from_file(oci_profile, bucket_file, namespace):
    """Clean up multiple buckets listed in a file"""
    # Load OCI config from specified profile
    config = oci.config.from_file(profile_name=oci_profile)
    object_storage_client = oci.object_storage.ObjectStorageClient(config)

    # Read bucket names from file
    try:
        with open(bucket_file, 'r') as f:
            buckets = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Error: Bucket list file '{bucket_file}' not found.")
        return
    except Exception as e:
        print(f"Error reading bucket list file: {e}")
        return

    if not buckets:
        print("No buckets found in the file.")
        return

    print(f"\nStarting cleanup of {len(buckets)} buckets...")

    # Create progress bar for buckets
    with tqdm(total=len(buckets), desc="Overall progress", position=0) as bucket_pbar:
        for bucket_name in buckets:
            clean_up_bucket(object_storage_client, bucket_name, namespace, bucket_pbar)


def main():
    # Set up CLI argument parsing
    parser = argparse.ArgumentParser(
        description="Clean up OCI buckets by deleting all objects and the buckets themselves")
    parser.add_argument("--oci_profile", required=True, help="OCI profile to use from the config file")
    parser.add_argument("--bucket_name", help="Single bucket name to clean up")
    parser.add_argument("--bucket_file", help="File containing list of buckets to clean up (one per line)")
    parser.add_argument("--max_retries", type=int, default=4, help="Maximum number of retry attempts")
    parser.add_argument("--retry_delay", type=int, default=10, help="Delay between retries in seconds")

    # Parse arguments
    args = parser.parse_args()

    if not args.bucket_name and not args.bucket_file:
        parser.error("Either --bucket_name or --bucket_file must be specified")

    if args.bucket_name and args.bucket_file:
        parser.error("Cannot specify both --bucket_name and --bucket_file")

    # Initialize OCI client
    config = oci.config.from_file(profile_name=args.oci_profile)
    object_storage_client = oci.object_storage.ObjectStorageClient(config)

    if args.bucket_file:
        clean_up_buckets_from_file(args.oci_profile, args.bucket_file, 'lrbvkel2wjot')
    else:
        clean_up_bucket(object_storage_client, args.bucket_name, 'lrbvkel2wjot')


if __name__ == "__main__":
    main()
