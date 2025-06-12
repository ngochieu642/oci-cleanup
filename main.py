import oci
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from threading import Lock
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_fixed
import click


def list_object_versions(object_storage_client, bucket_name, namespace):
    """List all object versions in a bucket with pagination"""
    try:
        all_objects = []
        next_page = None
        
        while True:
            response = object_storage_client.list_object_versions(
                namespace_name=namespace,
                bucket_name=bucket_name,
                page=next_page,
                limit=1000
            )
            
            if response.data.items:
                all_objects.extend(response.data.items)
            
            # Check if there are more items
            next_page = response.headers.get('opc-next-page')
            if not next_page:
                break
            
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
    """List all preauthenticated requests in a bucket with pagination"""
    try:
        all_pars = []
        next_page = None
        
        while True:
            response = object_storage_client.list_preauthenticated_requests(
                namespace_name=namespace,
                bucket_name=bucket_name,
                page=next_page,
                limit=1000
            )
            
            if response.data:
                all_pars.extend(response.data)
            
            # Check if there are more items
            next_page = response.headers.get('opc-next-page')
            if not next_page:
                break
            
        return all_pars
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


def list_multipart_uploads(object_storage_client, namespace, bucket_name):
    """List all multipart uploads in a bucket with pagination"""
    try:
        all_uploads = []
        next_page = None
        
        while True:
            response = object_storage_client.list_multipart_uploads(
                namespace_name=namespace,
                bucket_name=bucket_name,
                page=next_page,
                limit=1000
            )
            
            if response.data:
                all_uploads.extend(response.data)
            
            # Check if there are more items
            next_page = response.headers.get('opc-next-page')
            if not next_page:
                break
            
        return all_uploads
    except Exception as e:
        print(f"Error listing multipart uploads for bucket '{bucket_name}': {e}")
        return []


@retry(stop=stop_after_attempt(4), wait=wait_fixed(10))
def abort_multipart_upload_with_retry(object_storage_client, namespace, bucket_name, object_name, upload_id):
    """Abort a multipart upload with retry mechanism"""
    try:
        object_storage_client.abort_multipart_upload(
            namespace_name=namespace,
            bucket_name=bucket_name,
            object_name=object_name,
            upload_id=upload_id
        )
        return True
    except Exception as e:
        print(f"\nError aborting multipart upload {upload_id} for object {object_name}: {e}")
        return False


def delete_object_worker(object_storage_client, namespace, bucket_name, queue, progress_lock, obj_pbar):
    """Worker function to delete objects from the queue"""
    while True:
        try:
            item = queue.get_nowait()
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
                with progress_lock:
                    percentage = (obj_pbar.n + 1) / obj_pbar.total * 100
                    obj_pbar.set_postfix_str(f"[{percentage:.1f}%] Deleted: {object_name}")
                    obj_pbar.update(1)
            except Exception as e:
                print(f"\nError deleting {object_name} | Version ID: {version_id}: {e}")
                with progress_lock:
                    obj_pbar.update(1)
            finally:
                queue.task_done()

        except Empty:
            break


def clean_up_bucket(object_storage_client, bucket_name, namespace, bucket_pbar=None, delete_bucket=True, num_workers=1):
    """Delete all objects from a bucket and then delete the bucket itself if delete_bucket is True"""
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
        # Create queue and progress bar for object deletion
        object_queue = Queue()
        progress_lock = Lock()
        print(f"Running with {num_workers} workers")
        
        # Create progress bar with percentage format
        with tqdm(total=len(objects), desc=f"Deleting objects in {bucket_name}",
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}',
                 leave=False) as obj_pbar:
            
            # Fill the queue with objects
            for item in objects:
                object_queue.put(item)
            
            # Create and start worker threads
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                # Submit worker tasks
                futures = [
                    executor.submit(
                        delete_object_worker,
                        object_storage_client,
                        namespace,
                        bucket_name,
                        object_queue,
                        progress_lock,
                        obj_pbar
                    )
                    for _ in range(num_workers)
                ]
                
                # Wait for all tasks to complete
                for future in futures:
                    future.result()
                
                # Wait for queue to be empty
                object_queue.join()
    
    # Delete all preauthenticated requests
    pars = list_preauthenticated_requests(object_storage_client, namespace, bucket_name)
    if pars:
        with tqdm(total=len(pars), desc=f"Deleting preauthenticated requests in {bucket_name}",
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}',
                 leave=False) as par_pbar:
            for par in pars:
                try:
                    delete_par_with_retry(
                        object_storage_client,
                        namespace,
                        bucket_name,
                        par.id
                    )
                    percentage = (par_pbar.n + 1) / par_pbar.total * 100
                    par_pbar.set_postfix_str(f"[{percentage:.1f}%] Deleted PAR: {par.id}")
                except Exception as e:
                    print(f"\nError deleting PAR {par.id}: {e}")
                
                par_pbar.update(1)
    else:
        print(f"No preauthenticated requests found in bucket '{bucket_name}'")

    # Abort all multipart uploads
    multipart_uploads = list_multipart_uploads(object_storage_client, namespace, bucket_name)
    if multipart_uploads:
        with tqdm(total=len(multipart_uploads), desc=f"Aborting multipart uploads in {bucket_name}",
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}',
                 leave=False) as upload_pbar:
            for upload in multipart_uploads:
                try:
                    abort_multipart_upload_with_retry(
                        object_storage_client,
                        namespace,
                        bucket_name,
                        upload.object,
                        upload.upload_id
                    )
                    percentage = (upload_pbar.n + 1) / upload_pbar.total * 100
                    upload_pbar.set_postfix_str(f"[{percentage:.1f}%] Aborted upload: {upload.object}")
                except Exception as e:
                    print(f"\nError aborting multipart upload for {upload.object}: {e}")
                
                upload_pbar.update(1)
    else:
        print(f"No multipart uploads found in bucket '{bucket_name}'")
    
    # After all objects, PARs, and multipart uploads are deleted, delete the bucket if requested
    success = True
    if delete_bucket:
        success = delete_bucket_with_retry(object_storage_client, namespace, bucket_name)
    else:
        print(f"\nSkipping bucket deletion for '{bucket_name}' as requested")
    
    if bucket_pbar:
        bucket_pbar.update(1)
    
    return success


def clean_up_buckets_from_file(oci_profile, bucket_file, namespace, delete_bucket=True, num_workers=1):
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
            clean_up_bucket(object_storage_client, bucket_name, namespace, bucket_pbar, delete_bucket, num_workers)


@click.group()
def cli():
    """OCI cleanup utilities"""
    pass


@cli.command(name='clean-bucket')
@click.option('--oci-profile', required=True, help='OCI profile to use from the config file')
@click.option('--bucket-name', help='Single bucket name to clean up')
@click.option('--bucket-file', type=click.Path(exists=True), help='File containing list of buckets to clean up (one per line)')
@click.option('--max-retries', type=int, default=4, help='Maximum number of retry attempts')
@click.option('--retry-delay', type=int, default=10, help='Delay between retries in seconds')
@click.option('--delete-bucket/--no-delete-bucket', default=True, help='Delete the bucket after cleaning up its contents')
@click.option('--workers', type=int, default=1, help='Number of worker threads for parallel processing')
def clean_bucket(oci_profile, bucket_name, bucket_file, max_retries, retry_delay, delete_bucket, workers):
    """Clean up OCI buckets by deleting their contents and optionally the buckets themselves"""
    if not bucket_name and not bucket_file:
        raise click.UsageError("Either --bucket-name or --bucket-file must be specified")
    
    if bucket_name and bucket_file:
        raise click.UsageError("Cannot specify both --bucket-name and --bucket-file")

    # Initialize OCI client
    config = oci.config.from_file(profile_name=oci_profile)
    object_storage_client = oci.object_storage.ObjectStorageClient(config)

    if bucket_file:
        clean_up_buckets_from_file(oci_profile, bucket_file, 'lrbvkel2wjot', 
                                 delete_bucket, workers)
    else:
        clean_up_bucket(object_storage_client, bucket_name, 'lrbvkel2wjot', 
                       delete_bucket=delete_bucket, num_workers=workers)


if __name__ == "__main__":
    cli()
