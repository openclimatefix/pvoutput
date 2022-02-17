import os

import pytest

from nowcasting_datamodel.connection import Base_PV, DatabaseConnection

@pytest.fixture
def db_connection():

    url = os.getenv("DB_URL_PV", "sqlite:///test.db")

    connection = DatabaseConnection(url=url, base=Base_PV, echo=False)
    Base_PV.metadata.create_all(connection.engine)

    yield connection

    Base_PV.metadata.drop_all(connection.engine)


@pytest.fixture(scope="function", autouse=True)
def db_session(db_connection):
    """Creates a new database session for a test."""

    with db_connection.get_session() as s:
        s.begin()
        yield s
        s.rollback()
