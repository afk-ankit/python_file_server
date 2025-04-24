#!/usr/bin/env python3
import os
import argparse
import paramiko
import time
import hashlib


def calculate_md5(file_path):
    """Calculate MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def transfer_file(file_path, hostname, username, port, password, remote_dir, use_key=False, key_path=None):
    """Transfer a file to remote server using SFTP."""
    try:
        print(f"Connecting to {hostname}...")
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
            print(f"Creating remote directory: {remote_dir}")
            command = f"mkdir -p {remote_dir}"
            client.exec_command(command)

        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)
        remote_file_path = os.path.join(remote_dir, file_name)

        print(
            f"Starting transfer of {file_name} ({file_size/1024/1024:.2f} MB)...")
        start_time = time.time()

        # Perform the transfer
        sftp.put(file_path, remote_file_path, callback=lambda x, y: print(
            f"Transferred: {x/1024/1024:.2f} of {file_size/1024/1024:.2f} MB", end="\r"))

        transfer_time = time.time() - start_time
        transfer_speed = file_size / transfer_time / 1024 / 1024  # MB/s

        print("\nFile transfer complete!")
        print(f"Time taken: {transfer_time:.2f} seconds")
        print(f"Average transfer speed: {transfer_speed:.2f} MB/s")

        sftp.close()
        client.close()
        return True
    except Exception as e:
        print(f"Error transferring file: {str(e)}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Transfer a file using SFTP')
    parser.add_argument('file', help='File to transfer')
    parser.add_argument('--host', required=True, help='Remote host')
    parser.add_argument('--port', type=int, default=22, help='SSH port')
    parser.add_argument('--user', required=True, help='SSH username')
    parser.add_argument('--password', help='SSH password')
    parser.add_argument('--key', help='Path to SSH private key')
    parser.add_argument('--remote-dir', required=True,
                        help='Remote directory to transfer to')

    args = parser.parse_args()

    if not args.password and not args.key:
        parser.error("Either --password or --key must be provided")

    use_key = bool(args.key)

    # Calculate file hash before transfer
    print("Calculating MD5 hash of file...")
    original_md5 = calculate_md5(args.file)
    print(f"File MD5: {original_md5}")

    # Transfer the file
    transfer_file(
        args.file,
        args.host,
        args.user,
        args.port,
        args.password,
        args.remote_dir,
        use_key,
        args.key
    )

    print("\nTo verify the file integrity on the remote server, run:")
    print(f"ssh {args.user}@{args.host} \"md5sum {os.path.join(args.remote_dir, os.path.basename(args.file))}\"")
    print(f"And compare with the original MD5: {original_md5}")


if __name__ == "__main__":
    main()
