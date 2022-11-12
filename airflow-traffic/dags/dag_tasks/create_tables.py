from airflow.decorators import task
import logging
import os
from pathlib import Path


@task(task_id='create_tables')
def create_tables(db_session):
    """
    create 2 tables:
    1) Trajectories table
    2) Time frequencies table
    """
    dir_name = "/home/njogu-janerose/10-Academy-Repo/traffic-dwh-tech-stack/airflow-traffic"
    # access schema file in path
    sqlFile = dir_name+'/postgresql/db_schema.sql'
    # open and read file
    fd = open(sqlFile, 'r')
    readSqlFile = fd.read()
    fd.close()
    # retrieving SQL commands
    sqlCommands = readSqlFile.split(';')
    for command in sqlCommands:
        try:
            if (len(command) > 0):
                db_session.execute(command.strip())
                db_session.commit()
        except Exception as ex:
            logging.error("Command skipped: ", command)
            logging.error(ex)
    db_session.close()