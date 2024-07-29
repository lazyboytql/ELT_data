import subprocess
import time


def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True)
            if "accepting connections" in result.stdout:
                print("Successfully connected to PostgreSQL!")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            retries += 1
            print(
                f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    print("Max retries reached. Exiting.")
    return False


if not wait_for_postgres(host="source_postgres"):
    exit(1)

print("Starting ELT script...")

source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'startdb',
    'host': 'source_postgres'
}

final_config = {
    'dbname': 'final_db',
    'user': 'postgres',
    'password': 'finaldb',
    'host': 'final_postgres'
}

dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',

]

subprocess_env = dict(PGPASSWORD=source_config['password'])

subprocess.run(dump_command, env=subprocess_env, check=True)

load_command = [
    'psql',
    '-h', final_config['host'],
    '-U', final_config['user'],
    '-d', final_config['dbname'],
    '-a', '-f', 'data_dump.sql'
]

subprocess_env = dict(PGPASSWORD=final_config['password'])

subprocess.run(load_command, env=subprocess_env, check=True)

print("Ending ELT script...")