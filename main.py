from .services.OracleJob import Job
"""python Q:/master/master.py --config Q:/.secrets/.env -l ./logs -v --env homelab --exec python ./migration_job.py"""

def migrate(source_path: str="", object_name: str = ""):
    job=Job(
        source_path=source_path,
        table=object_name
    )
    result: int = job.run_job()
    return result

if __name__ == '__main__':
    migrate()