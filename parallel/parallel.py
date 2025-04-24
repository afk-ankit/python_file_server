#!/usr/bin/env python3
import os
import argparse
import math
import paramiko
import concurrent.futures
import time
from tqdm import tqdm
import hashlib


def split_file(file_path, chunk_size_mb=50):
    """Split a file into chunks of specified size."""
    chunk_size = chunk_size_mb * 1024 * 1024  # Convert MB to bytes
    file_size = os.path.getsize(file_path)
    num_chunks = math.ceil(file_size / chunk_size)

    chunks_dir = f"{file_path}_chunks"
    os.makedirs(chunks_dir, exist_ok=True)

    chunks = []
    with open(file_path, 'rb') as f:
        for i in range(num_chunks):
            chunk_file = os.path.join(chunks_dir, f"chunk_{i:03d}")
            chunk_data = f.read(chunk_size)
            with open(chunk_file, 'wb') as chunk_f:
                chunk_f.write(chunk_data)
            chunks.append(chunk_file)

    return chunks


def calculate_md5(file_path):
    """Calculate MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def transfer_chunk(chunk_file, hostname, username, port, password, remote_dir, use_key=False, key_path=None):
    """Transfer a single chunk to the remote server."""
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if use_key and key_path:
            key = paramiko.RSAKey.from_private_key_file(key_path)
            client.connect(hostname=hostname, username=username,
                           port=port, pkey=key)
        else:
            client.connect(hostname=hostname, username=username,
                           port=port, password=password)

        sftp = client.open_sftp()

        # Create remote directory if it doesn't exist
        try:
            sftp.stat(remote_dir)
        except FileNotFoundError:
            command = f"mkdir -p {remote_dir}"
            client.exec_command(command)

        remote_file = os.path.join(remote_dir, os.path.basename(chunk_file))
        # main transfer part
        sftp.put(chunk_file, remote_file)
        sftp.close()
        client.close()
        return True
    except Exception as e:
        print(f"Error transferring chunk {chunk_file}: {str(e)}")
        return False


def reassemble_chunks(hostname, username, port, password, remote_dir, original_file, use_key=False, key_path=None):
    """Reassemble chunks on the remote server."""
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if use_key and key_path:
            key = paramiko.RSAKey.from_private_key_file(key_path)
            client.connect(hostname=hostname, username=username,
                           port=port, pkey=key)
        else:
            client.connect(hostname=hostname, username=username,
                           port=port, password=password)

        remote_file = os.path.join(remote_dir, os.path.basename(original_file))
        # chunks_dir = f"{original_file}_chunks"

        # Create reassembly script on remote server
        temp_script = "/tmp/reassemble_script.sh"
        script_content = f"""#!/bin/bash
        cat {remote_dir}/chunk_* > {remote_file}
        rm -f {remote_dir}/chunk_*
        echo "File reassembled as {remote_file}"
        """

        stdin, stdout, stderr = client.exec_command(
            f"echo '{script_content}' > {temp_script}")
        stdin, stdout, stderr = client.exec_command(f"chmod +x {temp_script}")
        stdin, stdout, stderr = client.exec_command(f"bash {temp_script}")

        print(f"Output: {stdout.read().decode('utf-8')}")
        print(f"Errors: {stderr.read().decode('utf-8')}")

        # Clean up
        client.exec_command(f"rm {temp_script}")
        client.close()

        print(f"File reassembled on remote server as {remote_file}")
        return True
    except Exception as e:
        print(f"Error reassembling chunks: {str(e)}")
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
                        default=50, help='Chunk size in MB')
    parser.add_argument('--max-workers', type=int, default=5,
                        help='Maximum number of parallel transfers')

    args = parser.parse_args()

    if not args.password and not args.key:
        parser.error("Either --password or --key must be provided")

    use_key = bool(args.key)

    # Calculate file hash before transfer
    print("Calculating MD5 hash of original file...")
    original_md5 = calculate_md5(args.file)
    print(f"Original file MD5: {original_md5}")

    # Split the file
    print(f"Splitting file into chunks of {args.chunk_size}MB...")
    start_time = time.time()
    chunks = split_file(args.file, args.chunk_size)
    split_time = time.time() - start_time
    print(f"File split into {len(chunks)} chunks in {split_time:.2f} seconds")

    # Transfer chunks in parallel
    print("Transferring chunks in parallel...")
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = []
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
            futures.append(future)

        # Show progress
        with tqdm(total=len(chunks)) as pbar:
            for future in concurrent.futures.as_completed(futures):
                print(time.time())
                pbar.update(1)

    transfer_time = time.time() - start_time
    print(f"All chunks transferred in {transfer_time:.2f} seconds")

    # Reassemble the file on the remote server
    print("Reassembling file on remote server...")
    reassemble_chunks(
        args.host,
        args.user,
        args.port,
        args.password,
        args.remote_dir,
        args.file,
        use_key,
        args.key
    )

    # Clean up local chunks
    chunks_dir = f"{args.file}_chunks"
    print(f"Cleaning up local chunks in {chunks_dir}...")
    for chunk in chunks:
        os.remove(chunk)
    os.rmdir(chunks_dir)

    total_time = split_time + transfer_time
    print(f"Transfer complete! Total time: {total_time:.2f} seconds")

    print("\nTo verify the file integrity on the remote server, you can run:")
    print(f"ssh {args.user}@{args.host} \"md5sum {os.path.join(args.remote_dir, os.path.basename(args.file))}\"")
    print(f"And compare with the original MD5: {original_md5}")


if __name__ == "__main__":
    main()
