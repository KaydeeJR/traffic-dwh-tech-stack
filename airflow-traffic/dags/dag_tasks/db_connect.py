from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
"""
Connects to PostgreSQL database created by docker-compose
Opens a session to communicate with database
"""


def start_DB_session():
    """
    create a DB session
    connect to DB
    """
    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@localhost:5433/airflow", echo=True)
    db_session = scoped_session(sessionmaker(bind=engine))
    return db_session
