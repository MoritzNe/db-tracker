from .snapshot_manager import SnapshotManager
import pytest
import os
import inspect as pyinspect


def get_snapshot_dir():
    """
    Determine the directory of the currently running test and return its path.
    Snapshots will be stored in the same directory as the test file.
    """
    current_frame = pyinspect.currentframe()
    test_file = pyinspect.getfile(current_frame.f_back.f_back)  # Two frames up to get the test function
    test_dir = os.path.dirname(os.path.abspath(test_file))
    return os.path.join(test_dir, "snapshots")


def database_snapshot(setup_fixture=None, ignore_tables=None, diff_snapshots=False):
    """
    Decorator to capture and compare database snapshots or diffs with optional setup fixture,
    ability to ignore tables, and support for Syrupy snapshot assertions.
    """
    if ignore_tables is None:
        ignore_tables = []

    def decorator(test_function):
        @pytest.fixture
        def wrapper(request, *args, db_session=None, **kwargs):
            """
            Main wrapper function for the decorator.
            """
            # Access Syrupy's snapshot fixture using the pytest request object
            snapshot = request.getfixturevalue("snapshot")

            # Provide the session to the setup_fixture if available
            if setup_fixture:
                setup_fixture(db_session)

            # Initialize the snapshot manager
            snapshot_manager = SnapshotManager(
                db_session, ignore_tables, diff_snapshots
            )

            # Capture the "db_before" snapshot
            snapshot_manager.snapshot("db_before")

            # Run the test with the snapshot manager attached
            kwargs["snapshot_manager"] = snapshot_manager
            test_function(*args, db_session=db_session, **kwargs)

            # Capture the "db_after" snapshot
            snapshot_manager.snapshot("db_after")

            # Finalize and assert snapshots using Syrupy
            result_snapshots = snapshot_manager.finalize()
            assert snapshot == result_snapshots

        return wrapper
    return decorator
