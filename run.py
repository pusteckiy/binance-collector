import subprocess
import yaml
import sys
import os
from database import create_db

with open("config.yaml", "r") as stream:
    config = yaml.safe_load(stream)

python_interpreter = sys.executable

workers = []


def start_worker(pair):
    return subprocess.Popen([python_interpreter, os.path.join(os.getcwd(), "main.py"), pair])


def stop_all_workers():
    for worker in workers:
        worker.terminate()


if __name__ == "__main__":
    create_db()

    for pair in config["binance"]["symbols"]:
        worker = start_worker(pair)
        workers.append(worker)
        print("Started worker", pair)

    try:
        input("Press ENTER to stop workers")
    finally:
        stop_all_workers()
