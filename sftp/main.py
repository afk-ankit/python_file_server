import paramiko
import os
import time
import threading
from tqdm import tqdm  # For showing progress bar

# Reusable function to upload file via SFTP using paramiko


def sftp_upload(local_file, remote_path, hostname, username, password):
    # Start the timer
    start_time = time.time()

    # Connect to the SSH server
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, username=username, password=password)

    # Start SFTP session
    sftp = ssh.open_sftp()

    # Get file size for progress bar
    file_size = os.path.getsize(local_file)

    # Progress bar using tqdm (it shows a nice progress bar)
    with tqdm(total=file_size, unit='B', unit_scale=True, desc="Uploading file via SFTP") as pbar:
        # Use a callback to track the progress
        def progress_callback(transferred, total):
            pbar.update(transferred - pbar.n)

        # Upload the file using sftp.put and pass progress callback
        sftp.put(local_file, remote_path, callback=progress_callback)

    # Close the SFTP session and SSH connection
    sftp.close()
    ssh.close()

    # Calculate the elapsed time and print it
    elapsed_time = time.time() - start_time
    print(f"SFTP Upload completed in {elapsed_time:.2f} seconds")

# Reusable function for parallel file upload using threading


def parallel_upload(local_file, remote_path, hostname, username, password, chunk_size=104857600):
    # Start the timer
    start_time = time.time()

    # Connect to the SSH server
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, username=username, password=password)

    # Start SFTP session
    sftp = ssh.open_sftp()

    # Get file size for progress bar
    file_size = os.path.getsize(local_file)

    # Split the file into chunks
    def upload_chunk(chunk, remote_chunk_path):
        with open(chunk, 'rb') as f:
            sftp.putfo(f, remote_chunk_path)

    # List of threads for parallel upload
    threads = []
    num_chunks = file_size // chunk_size + (1 if file_size % chunk_size else 0)
    chunk_files = []

    # Create temporary chunk files
    with open(local_file, 'rb') as f:
        for i in range(num_chunks):
            chunk_path = f"temp_chunk_{i}"
            with open(chunk_path, 'wb') as chunk_file:
                chunk_file.write(f.read(chunk_size))
            chunk_files.append(chunk_path)

    # Start the threads for parallel upload
    with tqdm(total=file_size, unit='B', unit_scale=True, desc="Uploading file in parallel") as pbar:
        def thread_progress(chunk_idx, chunk_size):
            pbar.update(chunk_size)

        for idx, chunk in enumerate(chunk_files):
            remote_chunk_path = f"{remote_path}_part{idx}"
            thread = threading.Thread(
                target=upload_chunk, args=(chunk, remote_chunk_path))
            threads.append(thread)
            thread.start()

            # Update progress for each chunk
            threading.Thread(target=thread_progress,
                             args=(idx, chunk_size)).start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

    # Clean up chunk files
    for chunk in chunk_files:
        os.remove(chunk)

    # Close the SFTP session and SSH connection
    sftp.close()
    ssh.close()

    # Calculate the elapsed time and print it
    elapsed_time = time.time() - start_time
    print(f"Parallel Upload completed in {elapsed_time:.2f} seconds")

# Main function to execute both uploads one after the other


def main():
    local_path = './downloadables/myfile.zip'
    remote_path = "/scratch/malay/ankit_darta_testing/myfile.zip"
    hostname = os.getenv("HOSTNAME")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")

    # Upload via SFTP
    print("Starting SFTP Upload...")
    sftp_upload(local_path, remote_path, hostname, username, password)

    # # Upload via Parallel
    # print("Starting Parallel Upload...")
    # parallel_upload(local_file, remote_path, hostname, username, password)


if __name__ == "__main__":
    main()
