import paramiko
import argparse
from dotenv import load_dotenv
import os

load_dotenv()
host = os.getenv("HOST")
port = 22
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")
tmux_session = "bot"


def ssh_connect():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, port, username, password)
    return client


def install_file():
    try:
        client = ssh_connect()
        sftp = client.open_sftp()
        sftp.put("./main.py", "/home/main.py")
        sftp.put("./allowed.txt", "/home/allowed.txt")
        sftp.close()
        print(f"Bot updated")
        client.close()
    except Exception as e:
        print(f"Error: {e}")


def start_session():
    try:
        client = ssh_connect()
        commands = [
            f"tmux new-session -d -s {tmux_session}",
            f"tmux send-keys -t {tmux_session} 'python main.py' C-m",
            f"tmux detach -t {tmux_session}"
        ]
        for cmd in commands:
            stdin, stdout, stderr = client.exec_command(cmd)
            stdout.channel.recv_exit_status()
        print("Bot started")
        client.close()
    except Exception as e:
        print(f"Error: {e}")

def install_package(package):
    try:
        client = ssh_connect()
        stdin, stdout, stderr = client.exec_command(f"pip install {package}")
        stdout.channel.recv_exit_status()
        print(f"{package} installed")
        client.close()
    except Exception as e:
        print(f"Error: {e}")

def stop_session():
    try:
        client = ssh_connect()
        cmd = f"tmux kill-session -t {tmux_session}"
        stdin, stdout, stderr = client.exec_command(cmd)
        stdout.channel.recv_exit_status()
        print("Bot stopped")
        client.close()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["install", "start", "stop", "pkg"], help="install, start, stop, pkg -p [package]")
    parser.add_argument('-p', '--package')
    args = parser.parse_args()

    if args.action == "install":
        install_file()
    elif args.action == "start":
        start_session()
    elif args.action == "stop":
        stop_session()
    elif args.action == "pkg":
        install_package(args.package)