import oci
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from threading import Lock

from oci.log_analytics import LogAnalyticsClient
from oci.object_storage import ObjectStorageClient
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


def list_log_analytics_entities(log_analytics_client: LogAnalyticsClient, compartment_id: str, namespace: str):
    """List all log analytics entities in a compartment with pagination"""
    try:
        all_entities = []
        next_page = None
        
        while True:
            response = log_analytics_client.list_log_analytics_entities(
                namespace_name=namespace,
                compartment_id=compartment_id,
                page=next_page,
                limit=1000
            )

            if response.data.items:
                all_entities.extend(response.data.items)
            
            # Check if there are more items
            next_page = response.headers.get('opc-next-page')
            if not next_page:
                break
            
        return all_entities
    except Exception as e:
        print(f"Error listing log analytics entities: {e}")
        return []


@retry(stop=stop_after_attempt(4), wait=wait_fixed(10))
def delete_log_analytics_entity_with_retry(log_analytics_client, namespace, entity_id):
    """Delete a log analytics entity with retry mechanism"""
    try:
        log_analytics_client.delete_entity(
            namespace_name=namespace,
            entity_id=entity_id
        )
        return True
    except Exception as e:
        print(f"\nError deleting log analytics entity {entity_id}: {e}")
        return False


def clean_log_analytics_entities(log_analytics_client: LogAnalyticsClient, compartment_id: str, namespace: str, num_workers=1):
    """Delete all log analytics entities in a compartment"""
    # Get list of log analytics entities
    entities = list_log_analytics_entities(log_analytics_client, compartment_id, namespace)
    
    if not entities:
        print(f"No log analytics entities found in compartment '{compartment_id}'")
        return
    
    print(f"\nFound {len(entities)} log analytics entities to delete")
    
    # Create queue and progress bar for entity deletion
    entity_queue = Queue()
    progress_lock = Lock()
    print(f"Running with {num_workers} workers")
    
    # Create progress bar with percentage format
    with tqdm(total=len(entities), desc="Deleting log analytics entities",
             bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}',
             leave=False) as entity_pbar:
        
        # Fill the queue with entities
        for entity in entities:
            entity_queue.put(entity)
        
        # Create and start worker threads
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit worker tasks
            futures = [
                executor.submit(
                    delete_log_analytics_entity_worker,
                    log_analytics_client,
                    namespace,
                    entity_queue,
                    progress_lock,
                    entity_pbar
                )
                for _ in range(num_workers)
            ]
            
            # Wait for all tasks to complete
            for future in futures:
                future.result()
            
            # Wait for queue to be empty
            entity_queue.join()


def delete_log_analytics_entity_worker(log_analytics_client, namespace, queue, progress_lock, entity_pbar):
    """Worker function to delete log analytics entities from the queue"""
    while True:
        try:
            entity = queue.get_nowait()
            entity_id = entity.id
            entity_name = entity.name

            try:
                delete_log_analytics_entity_with_retry(
                    log_analytics_client,
                    namespace,
                    entity_id
                )
                with progress_lock:
                    percentage = (entity_pbar.n + 1) / entity_pbar.total * 100
                    entity_pbar.set_postfix_str(f"[{percentage:.1f}%] Deleted: {entity_name}")
                    entity_pbar.update(1)
            except Exception as e:
                print(f"\nError deleting entity {entity_name} | ID: {entity_id}: {e}")
                with progress_lock:
                    entity_pbar.update(1)
            finally:
                queue.task_done()

        except Empty:
            break


@cli.command(name='clean-logs-analytics')
@click.option('--oci-profile', required=True, help='OCI profile to use from the config file')
@click.option('--compartment-id', required=True, help='Compartment ID containing the log analytics entities')
@click.option('--workers', type=int, default=1, help='Number of worker threads for parallel processing')
def clean_logs_analytics(oci_profile, compartment_id, workers):
    """Clean up OCI Log Analytics entities in a compartment"""
    # Initialize OCI clients
    config = oci.config.from_file(profile_name=oci_profile)
    log_analytics_client: LogAnalyticsClient = oci.log_analytics.LogAnalyticsClient(config)
    object_storage_client: ObjectStorageClient = oci.object_storage.ObjectStorageClient(config)
    
    # Get namespace using Object Storage client
    try:
        namespace_response = object_storage_client.get_namespace()
        namespace = namespace_response.data
    except Exception as e:
        raise click.UsageError(f"Failed to get namespace: {e}")

    clean_log_analytics_entities(log_analytics_client, compartment_id, namespace, workers)


if __name__ == "__main__":
    cli()
