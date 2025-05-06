#!/usr/bin/env python3
import os
import argparse
import math
import paramiko
import concurrent.futures
import time
import hashlib
import threading
import psutil
import logging
import datetime


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('parallel_transfer.log')
    ]
)
logger = logging.getLogger('parallel_transfer')

# Create a global progress dictionary to track individual chunks' progress
chunk_progress = {}
progress_lock = threading.Lock()
transfer_stats = {}  # Store transfer statistics for reporting


def calculate_optimal_parameters(file_path, manual_chunk_size=None, manual_workers=None):
    """
    Calculate optimal chunk size and worker count based on file size and system resources
    Returns tuple of (chunk_size_mb, worker_count)
    """
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)

    # If manual parameters are provided, use them
    if manual_chunk_size is not None and manual_workers is not None:
        return manual_chunk_size, manual_workers

    # CPU cores available
    cpu_count = psutil.cpu_count(logical=False)  # Physical cores
    if cpu_count is None:
        cpu_count = psutil.cpu_count(logical=True)  # Logical cores as fallback
    if cpu_count is None:
        cpu_count = 4  # Default if we can't determine

    # Get available memory in MB
    available_memory_mb = psutil.virtual_memory().available / (1024 * 1024)

    # Calculate optimal worker count (scale with CPU cores but cap it)
    if manual_workers is not None:
        worker_count = manual_workers
    else:
        # Start with cpu_count, but scale down for very small files
        worker_count = min(cpu_count, max(2, int(file_size_mb / 100)))
        # Cap at 10 workers to avoid overwhelming network
        worker_count = min(10, worker_count)

    # Calculate optimal chunk size
    if manual_chunk_size is not None:
        chunk_size_mb = manual_chunk_size
    else:
        # For very small files, use smaller chunks
        if file_size_mb < 50:
            chunk_size_mb = max(1, file_size_mb / 5)
        # For medium files
        elif file_size_mb < 1000:  # less than 1GB
            chunk_size_mb = 50
        # For large files, increase chunk size, but ensure reasonable number of chunks
        else:
            # Target at least 20 chunks for large files
            chunk_size_mb = max(50, min(500, file_size_mb / 20))

        # Since we're now using streaming for file splitting, memory requirements are much lower
        # We mainly need memory for transfer buffers which are much smaller
        # We still want to be conservative but can be less restrictive
        buffer_memory_per_worker = 20  # Estimate 20MB per worker for buffers and overhead
        total_buffer_memory = worker_count * buffer_memory_per_worker

        # Make sure we have enough memory for basic operations plus some safety margin
        if total_buffer_memory > available_memory_mb * 0.7:  # Using 70% of available memory as limit
            # If we don't have enough memory, reduce worker count
            worker_count = max(
                2, int((available_memory_mb * 0.7) / buffer_memory_per_worker))

        # Chunk size can be larger now since we're streaming
        # But we still want reasonable sized chunks for network reliability
        # Cap at 2GB per chunk maximum
        chunk_size_mb = min(chunk_size_mb, 2000)

        # Round to nearest 5MB for cleaner numbers
        chunk_size_mb = math.ceil(chunk_size_mb / 5) * 5

    return int(chunk_size_mb), worker_count


def split_file(file_path, chunk_size_mb=50):
    """Split a file into chunks of specified size using streaming to minimize memory usage."""
    chunk_size = chunk_size_mb * 1024 * 1024  # Convert MB to bytes
    file_size = os.path.getsize(file_path)
    num_chunks = math.ceil(file_size / chunk_size)

    # Use a smaller buffer size for streaming (8MB default)
    buffer_size = min(8 * 1024 * 1024, chunk_size)

    chunks_dir = f"{file_path}_chunks"
    os.makedirs(chunks_dir, exist_ok=True)

    chunks = []
    try:
        with open(file_path, 'rb') as f:
            for i in range(num_chunks):
                chunk_file = os.path.join(chunks_dir, f"chunk_{i:03d}")
                chunks.append(chunk_file)

                # Stream the chunk in smaller buffer pieces
                bytes_written = 0
                bytes_remaining = min(chunk_size, file_size - i * chunk_size)

                with open(chunk_file, 'wb') as chunk_f:
                    while bytes_written < bytes_remaining:
                        # Calculate how much to read in this iteration
                        current_buffer_size = min(
                            buffer_size, bytes_remaining - bytes_written)

                        # Read and write a buffer-sized piece
                        buf = f.read(current_buffer_size)
                        if not buf:  # End of file
                            break

                        chunk_f.write(buf)
                        bytes_written += len(buf)

        logger.info(f"Successfully split file into {len(chunks)} chunks")
        return chunks
    except Exception as e:
        logger.error(f"Error splitting file: {str(e)}", exc_info=True)
        # Clean up any partial chunks
        for chunk in chunks:
            if os.path.exists(chunk):
                os.remove(chunk)
        if os.path.exists(chunks_dir) and not os.listdir(chunks_dir):
            os.rmdir(chunks_dir)
        raise


def calculate_md5(file_path):
    """Calculate MD5 hash of a file."""
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logger.error(
            f"Error calculating MD5 for {file_path}: {str(e)}", exc_info=True)
        raise


class ProgressCallback:
    def __init__(self, chunk_file, file_size):
        self.chunk_name = os.path.basename(chunk_file)
        self.file_size = file_size
        self.transferred = 0
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.last_bytes = 0
        self.current_speed = 0  # Speed in bytes/second

        # Initialize progress tracking for this chunk
        with progress_lock:
            chunk_progress[self.chunk_name] = {
                'size': file_size,
                'transferred': 0,
                'percent': 0,
                'speed': 0,  # Speed in bytes/second
                'start_time': self.start_time
            }

    def __call__(self, transferred, remaining):
        current_time = time.time()
        time_diff = current_time - self.last_update_time

        # Only calculate speed if time difference is significant
        if time_diff > 0.5:  # Update speed every half second
            bytes_diff = transferred - self.last_bytes
            self.current_speed = bytes_diff / time_diff
            self.last_update_time = current_time
            self.last_bytes = transferred

        # Update tracking information
        self.transferred = transferred
        percent = int(100 * transferred /
                      self.file_size) if self.file_size > 0 else 0

        # Update the global progress dictionary with thread safety
        with progress_lock:
            chunk_progress[self.chunk_name] = {
                'size': self.file_size,
                'transferred': transferred,
                'percent': percent,
                'speed': self.current_speed,
                'start_time': self.start_time
            }


def transfer_chunk(chunk_file, hostname, username, port, password, remote_dir, use_key=False, key_path=None):
    """Transfer a single chunk to the remote server with progress tracking."""
    chunk_name = os.path.basename(chunk_file)
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            if use_key and key_path:
                try:
                    key = paramiko.RSAKey.from_private_key_file(key_path)
                    client.connect(hostname=hostname, username=username,
                                   port=port, pkey=key)
                except Exception as e:
                    raise Exception(f"Failed to use SSH key: {str(e)}")
            else:
                client.connect(hostname=hostname, username=username,
                               port=port, password=password)
        except paramiko.AuthenticationException:
            logger.error(f"Authentication failed for {hostname}")
            return False
        except paramiko.SSHException as e:
            logger.error(f"SSH connection error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Connection error: {str(e)}")
            return False

        try:
            sftp = client.open_sftp()
        except Exception as e:
            logger.error(f"Failed to establish SFTP session: {str(e)}")
            client.close()
            return False

        # Create remote directory if it doesn't exist
        try:
            sftp.stat(remote_dir)
        except FileNotFoundError:
            try:
                command = f"mkdir -p {remote_dir}"
                stdin, stdout, stderr = client.exec_command(command)
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    error = stderr.read().decode('utf-8')
                    logger.error(f"Failed to create remote directory: {error}")
                    sftp.close()
                    client.close()
                    return False
            except Exception as e:
                logger.error(f"Failed to create remote directory: {str(e)}")
                sftp.close()
                client.close()
                return False

        remote_file = os.path.join(remote_dir, chunk_name)

        # Set up progress callback
        file_size = os.path.getsize(chunk_file)
        callback = ProgressCallback(chunk_file, file_size)
        start_time = time.time()

        try:
            # Use the callback during transfer
            sftp.put(chunk_file, remote_file, callback=callback)
        except Exception as e:
            logger.error(f"Failed to transfer {chunk_name}: {str(e)}")
            sftp.close()
            client.close()
            return False

        sftp.close()
        client.close()

        # Calculate transfer statistics
        end_time = time.time()
        duration = end_time - start_time
        avg_speed = (file_size / duration) if duration > 0 else 0

        # Mark complete in progress tracking and store stats
        with progress_lock:
            if chunk_name in chunk_progress:
                del chunk_progress[chunk_name]

            # Store transfer statistics
            transfer_stats[chunk_name] = {
                'size': file_size,
                'duration': duration,
                'avg_speed': avg_speed,
                'start_time': start_time,
                'end_time': end_time
            }

        logger.info(
            f"Successfully transferred {chunk_name} - Avg speed: {format_speed(avg_speed)}")
        return True
    except Exception as e:
        logger.error(
            f"Error transferring chunk {chunk_name}: {str(e)}", exc_info=True)
        return False


def format_speed(speed_bytes_per_sec):
    """Format speed in human-readable form."""
    if speed_bytes_per_sec < 1024:
        return f"{speed_bytes_per_sec:.2f} B/s"
    elif speed_bytes_per_sec < 1024 * 1024:
        return f"{speed_bytes_per_sec / 1024:.2f} KB/s"
    elif speed_bytes_per_sec < 1024 * 1024 * 1024:
        return f"{speed_bytes_per_sec / (1024 * 1024):.2f} MB/s"
    else:
        return f"{speed_bytes_per_sec / (1024 * 1024 * 1024):.2f} GB/s"


def print_progress():
    """Print progress of individual chunks periodically."""
    while True:
        # Clear screen (works in most terminals)
        os.system('cls' if os.name == 'nt' else 'clear')

        print("Current chunk transfer progress:")
        print("-" * 80)

        with progress_lock:
            if not chunk_progress:  # If dictionary is empty
                print("No transfers in progress...")
            else:
                # Sort chunks by name for consistent display
                for chunk_name in sorted(chunk_progress.keys()):
                    info = chunk_progress[chunk_name]
                    progress_bar = '█' * \
                        int(info['percent'] / 2) + '░' * \
                        (50 - int(info['percent'] / 2))
                    transferred_mb = info['transferred'] / (1024 * 1024)
                    total_mb = info['size'] / (1024 * 1024)
                    speed_str = format_speed(info['speed'])
                    elapsed = time.time() - info['start_time']

                    print(
                        f"{chunk_name}: [{progress_bar}] {info['percent']}% " +
                        f"({transferred_mb:.2f}MB/{total_mb:.2f}MB) " +
                        f"Speed: {speed_str} Time: {elapsed:.1f}s"
                    )

        print("-" * 80)
        time.sleep(0.5)  # Update every half second

        # Check if we should exit the thread
        with progress_lock:
            if getattr(threading.current_thread(), "stop_flag", False):
                break


def print_transfer_summary():
    """Print a summary of all transfers."""
    print("\nTransfer Summary:")
    print("-" * 100)
    print(f"{'Chunk':<15} {'Size':<12} {'Duration':<12} {'Avg Speed':<15} {'Start Time':<25} {'End Time':<25}")
    print("-" * 100)

    total_size = 0
    total_duration = 0

    # Sort by chunk name for consistent output
    for chunk_name in sorted(transfer_stats.keys()):
        stats = transfer_stats[chunk_name]
        size_mb = stats['size'] / (1024 * 1024)
        speed_str = format_speed(stats['avg_speed'])
        start_time_str = datetime.datetime.fromtimestamp(
            stats['start_time']).strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = datetime.datetime.fromtimestamp(
            stats['end_time']).strftime('%Y-%m-%d %H:%M:%S')

        print(
            f"{chunk_name:<15} {size_mb:.2f} MB{'':<4} {stats['duration']:.2f} s{'':<4} {speed_str:<15} {start_time_str:<25} {end_time_str:<25}")

        total_size += stats['size']
        total_duration += stats['duration']

    print("-" * 100)

    # Calculate overall statistics
    total_size_mb = total_size / (1024 * 1024)
    avg_overall_speed = total_size / total_duration if total_duration > 0 else 0
    avg_speed_str = format_speed(avg_overall_speed)

    print(
        f"Total: {total_size_mb:.2f} MB transferred in {total_duration:.2f} seconds (Average: {avg_speed_str})")
    print("-" * 100)

    # Log the summary
    logger.info(
        f"Transfer completed: {total_size_mb:.2f} MB transferred in {total_duration:.2f} seconds (Average: {avg_speed_str})")


def reassemble_chunks(hostname, username, port, password, remote_dir, original_file, use_key=False, key_path=None):
    """Reassemble chunks on the remote server."""
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            if use_key and key_path:
                key = paramiko.RSAKey.from_private_key_file(key_path)
                client.connect(hostname=hostname, username=username,
                               port=port, pkey=key)
            else:
                client.connect(hostname=hostname, username=username,
                               port=port, password=password)
        except paramiko.AuthenticationException:
            logger.error(f"Authentication failed for {hostname}")
            return False
        except paramiko.SSHException as e:
            logger.error(f"SSH connection error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Connection error: {str(e)}")
            return False

        remote_file = os.path.join(remote_dir, os.path.basename(original_file))

        # Create reassembly script on remote server
        temp_script = "/tmp/reassemble_script.sh"
        script_content = f"""#!/bin/bash
        cat {remote_dir}/chunk_* > {remote_file}
        rm -f {remote_dir}/chunk_*
        echo "File reassembled as {remote_file}"
        """

        try:
            stdin, stdout, stderr = client.exec_command(
                f"echo '{script_content}' > {temp_script}")
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                error = stderr.read().decode('utf-8')
                logger.error(f"Failed to create reassembly script: {error}")
                client.close()
                return False

            stdin, stdout, stderr = client.exec_command(
                f"chmod +x {temp_script}")
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                error = stderr.read().decode('utf-8')
                logger.error(f"Failed to make script executable: {error}")
                client.close()
                return False

            stdin, stdout, stderr = client.exec_command(f"bash {temp_script}")
            exit_status = stdout.channel.recv_exit_status()

            output = stdout.read().decode('utf-8')
            errors = stderr.read().decode('utf-8')

            if exit_status != 0:
                logger.error(f"Reassembly failed: {errors}")
                client.close()
                return False

            logger.info(f"Reassembly output: {output}")
            if errors:
                logger.warning(f"Reassembly warnings: {errors}")
        except Exception as e:
            logger.error(f"Error during reassembly: {str(e)}", exc_info=True)
            client.close()
            return False

        # Clean up
        try:
            client.exec_command(f"rm {temp_script}")
        except Exception as e:
            logger.warning(f"Failed to remove temporary script: {str(e)}")

        client.close()

        logger.info(f"File reassembled on remote server as {remote_file}")
        return True
    except Exception as e:
        logger.error(f"Error reassembling chunks: {str(e)}", exc_info=True)
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Split and transfer large files in parallel')
    parser.add_argument('file', help='File to transfer')
    parser.add_argument('--host', required=True, help='Remote host')
    parser.add_argument('--port', type=int, default=22, help='SSH port')
    parser.add_argument('--user', required=True, help='SSH username')
    parser.add_argument('--password', help='SSH password')
    parser.add_argument('--key', help='Path to SSH private key')
    parser.add_argument('--remote-dir', required=True,
                        help='Remote directory to transfer to')
    parser.add_argument('--chunk-size', type=int,
                        help='Chunk size in MB (optional, will be calculated automatically if not provided)')
    parser.add_argument('--max-workers', type=int,
                        help='Maximum number of parallel transfers (optional, will be calculated automatically if not provided)')
    parser.add_argument('--auto', action='store_true',
                        help='Use automatic parameter detection (default if chunk-size and max-workers are not specified)')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='INFO', help='Set logging level')

    args = parser.parse_args()

    # Set log level
    logger.setLevel(getattr(logging, args.log_level))

    if not args.password and not args.key:
        parser.error("Either --password or --key must be provided")

    use_key = bool(args.key)
    file_path = args.file

    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return 1

    try:
        # Calculate file hash before transfer
        logger.info("Calculating MD5 hash of original file...")
        original_md5 = calculate_md5(file_path)
        logger.info(f"Original file MD5: {original_md5}")

        # Calculate optimal parameters
        chunk_size_mb, max_workers = calculate_optimal_parameters(
            file_path, args.chunk_size, args.max_workers)

        file_size_bytes = os.path.getsize(file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
        file_size_gb = file_size_mb / 1024

        # Print human-readable file size
        if file_size_gb >= 1:
            logger.info(f"File size: {file_size_gb:.2f} GB")
        else:
            logger.info(f"File size: {file_size_mb:.2f} MB")

        # Print memory usage estimate
        buffer_memory_per_worker = 20  # MB, estimated overhead per worker
        estimated_memory = max_workers * buffer_memory_per_worker  # MB
        available_memory_mb = psutil.virtual_memory().available / (1024 * 1024)

        logger.info(
            f"Using chunk size: {chunk_size_mb} MB (streaming with 8MB buffers)")
        logger.info(f"Using worker count: {max_workers}")
        logger.info(
            f"Estimated memory usage: {estimated_memory:.1f} MB (of {available_memory_mb:.1f} MB available)")
        logger.info(
            f"Number of chunks: {math.ceil(file_size_mb / chunk_size_mb)}")

        # Split the file
        logger.info(f"Splitting file into chunks of {chunk_size_mb}MB...")
        start_time = time.time()
        chunks = split_file(file_path, chunk_size_mb)
        split_time = time.time() - start_time
        logger.info(
            f"File split into {len(chunks)} chunks in {split_time:.2f} seconds")

        # Start progress display thread
        progress_thread = threading.Thread(target=print_progress)
        progress_thread.daemon = True  # Thread will exit when main thread exits
        progress_thread.start()

        # Transfer chunks in parallel
        logger.info("Transferring chunks in parallel...")
        start_time = time.time()
        failed_chunks = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for chunk in chunks:
                future = executor.submit(
                    transfer_chunk,
                    chunk,
                    args.host,
                    args.user,
                    args.port,
                    args.password,
                    args.remote_dir,
                    use_key,
                    args.key
                )
                futures[future] = chunk

            # Track completion
            completed = 0
            total = len(chunks)
            for future in concurrent.futures.as_completed(futures):
                chunk = futures[future]
                success = future.result()
                completed += 1

                if not success:
                    failed_chunks.append(os.path.basename(chunk))
                    logger.error(
                        f"Failed to transfer chunk: {os.path.basename(chunk)}")

                logger.info(
                    f"Overall progress: {completed}/{total} chunks completed ({int(completed/total*100)}%)")

        # Signal the progress thread to stop
        setattr(progress_thread, "stop_flag", True)
        progress_thread.join(timeout=1.0)

        transfer_time = time.time() - start_time
        logger.info(f"All chunks processed in {transfer_time:.2f} seconds")

        # Print transfer summary
        print_transfer_summary()

        # Check if there were any failed transfers
        if failed_chunks:
            logger.error(
                f"Failed to transfer {len(failed_chunks)} chunks: {', '.join(failed_chunks)}")
            logger.error("Aborting reassembly due to failed transfers")
            return 1

        # Reassemble the file on the remote server
        logger.info("Reassembling file on remote server...")
        reassembly_success = reassemble_chunks(
            args.host,
            args.user,
            args.port,
            args.password,
            args.remote_dir,
            file_path,
            use_key,
            args.key
        )

        if not reassembly_success:
            logger.error("Failed to reassemble file on remote server")
            return 1

        # Clean up local chunks
        chunks_dir = f"{file_path}_chunks"
        logger.info(f"Cleaning up local chunks in {chunks_dir}...")
        for chunk in chunks:
            try:
                os.remove(chunk)
            except Exception as e:
                logger.warning(f"Failed to remove chunk {chunk}: {str(e)}")

        try:
            os.rmdir(chunks_dir)
        except Exception as e:
            logger.warning(f"Failed to remove chunks directory: {str(e)}")

        total_time = split_time + transfer_time
        logger.info(f"Transfer complete! Total time: {total_time:.2f} seconds")

        logger.info(
            "\nTo verify the file integrity on the remote server, you can run:")
        logger.info(
            f"ssh {args.user}@{args.host} \"md5sum {os.path.join(args.remote_dir, os.path.basename(file_path))}\"")
        logger.info(f"And compare with the original MD5: {original_md5}")

        return 0
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
