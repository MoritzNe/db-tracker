import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from db_tracker.database_tracker import database_snapshot


# Database configuration
DATABASE_URL = "sqlite:///:memory:"  # Use an in-memory SQLite database for simplicity
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


@pytest.fixture(scope="module")
def db_engine():
    """
    Provide a database engine for the tests.
    """
    yield engine
    engine.dispose()

@pytest.fixture(scope="function")
def db_session(db_engine):
    """
    Provide a database session for each test, rolling back changes after the test.
    """
    connection = db_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="module", autouse=True)
def setup_database(db_engine):
    """
    Set up the database schema and initial data.
    """
    with db_engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE important_table (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE ignored_table (
                id INTEGER PRIMARY KEY,
                value TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE dynamic_table (
                id INTEGER PRIMARY KEY,
                description TEXT
            );
        """))


@pytest.fixture
def setup_data(db_session):
    """
    Populate the database with initial test data.
    """
    db_session.execute(text("INSERT INTO important_table (id, name) VALUES (1, 'Initial Name');"))
    db_session.commit()


@database_snapshot(setup_fixture=setup_data, ignore_tables=["ignored_table"])
def test_snapshot_basic(db_session, snapshot_manager):
    """
    Test basic snapshot functionality.
    """
    # Take a snapshot after initial setup
    snapshot_manager.snapshot("after_setup")

    # Update the database
    db_session.execute(text("UPDATE important_table SET name = 'Updated Name' WHERE id = 1;"))
    db_session.commit()

    # Take another snapshot
    snapshot_manager.snapshot("after_update")

    # Verify the updated value
    result = db_session.execute(text("SELECT name FROM important_table WHERE id = 1")).fetchone()
    assert result[0] == "Updated Name"


@database_snapshot(setup_fixture=setup_data, ignore_tables=["ignored_table"], diff_snapshots=True)
def test_snapshot_with_diff(db_session, snapshot_manager):
    """
    Test diff snapshot functionality.
    """
    # Update the database
    db_session.execute(text("INSERT INTO important_table (id, name) VALUES (2, 'New Entry');"))
    db_session.commit()

    # Take a snapshot after the update
    snapshot_manager.snapshot("after_insert")

    # Remove the newly added row
    db_session.execute(text("DELETE FROM important_table WHERE id = 2;"))
    db_session.commit()

    # Take another snapshot
    snapshot_manager.snapshot("after_delete")

    # Verify that the diff captures additions and deletions correctly
    result = db_session.execute(text("SELECT COUNT(*) FROM important_table")).scalar()
    assert result == 1  # Only the original row remains


@database_snapshot(setup_fixture=setup_data, ignore_tables=["ignored_table"])
def test_empty_table_behavior(db_session, snapshot_manager):
    """
    Test that empty tables are correctly handled in snapshots.
    """
    # Take a snapshot with the dynamic table empty
    snapshot_manager.snapshot("empty_dynamic_table")

    # Add data to the dynamic table
    db_session.execute(text("INSERT INTO dynamic_table (id, description) VALUES (1, 'Dynamic Data');"))
    db_session.commit()

    # Take another snapshot
    snapshot_manager.snapshot("non_empty_dynamic_table")

    # Ensure the dynamic table data is included in the second snapshot
    result = db_session.execute(text("SELECT COUNT(*) FROM dynamic_table")).scalar()
    assert result == 1


@database_snapshot(setup_fixture=setup_data, ignore_tables=[])
def test_no_ignore_tables(db_session, snapshot_manager):
    """
    Test that all tables are included when no ignore tables are specified.
    """
    # Insert data into the ignored table
    db_session.execute(text("INSERT INTO ignored_table (id, value) VALUES (1, 'Ignored Data');"))
    db_session.commit()

    # Take a snapshot
    snapshot_manager.snapshot("snapshot_with_ignored_table")

    # Verify the ignored table data exists in the snapshot
    result = db_session.execute(text("SELECT COUNT(*) FROM ignored_table")).scalar()
    assert result == 1


@database_snapshot(setup_fixture=setup_data, ignore_tables=["ignored_table"])
def test_multiple_snapshots(db_session, snapshot_manager):
    """
    Test capturing multiple snapshots at different stages.
    """
    # Take the initial snapshot
    snapshot_manager.snapshot("initial_state")

    # Modify the important table
    db_session.execute(text("UPDATE important_table SET name = 'Intermediate Name' WHERE id = 1;"))
    db_session.commit()

    # Take an intermediate snapshot
    snapshot_manager.snapshot("intermediate_state")

    # Add a new entry
    db_session.execute(text("INSERT INTO important_table (id, name) VALUES (2, 'Final Name');"))
    db_session.commit()

    # Take the final snapshot
    snapshot_manager.snapshot("final_state")

    # Verify the final state of the database
    result = db_session.execute(text("SELECT COUNT(*) FROM important_table")).scalar()
    assert result == 2
