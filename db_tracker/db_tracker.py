from sqlalchemy import inspect


class SnapshotManager:
    """
    A helper object to manage snapshots and diffs within the test.
    """
    def __init__(self, db_session, test_name, ignore_tables, matchers, diff_snapshots):
        self.db_session = db_session
        self.test_name = test_name
        self.ignore_tables = ignore_tables
        self.matchers = matchers
        self.diff_snapshots = diff_snapshots
        self.snapshots = []

    def snapshot(self, name):
        """
        Capture a snapshot with the given name, excluding empty tables.
        """
        state = self.fetch_database_state()
        if state:
            self.snapshots.append((name, state))

    def fetch_database_state(self):
        """
        Fetch the database state using SQLAlchemy, excluding empty tables and ignored tables.
        """
        inspector = inspect(self.db_session.bind)
        tables = inspector.get_table_names()

        # Filter out ignored tables
        tables_to_include = [table for table in tables if table not in self.ignore_tables]

        state = {}
        for table in tables_to_include:
            rows = self.db_session.execute(f"SELECT * FROM {table}").fetchall()
            if rows:  # Skip empty tables
                columns = rows[0].keys()
                state[table] = [dict(zip(columns, row)) for row in rows]
        return state

    def calculate_diff(self, previous, current):
        """
        Calculate the diff between two snapshots at the table level.
        - Added rows: Present in `current` but not in `previous`.
        - Removed rows: Present in `previous` but not in `current`.
        """
        diff = {}
        all_tables = set(previous.keys()).union(set(current.keys()))
        for table in all_tables:
            prev_rows = {tuple(row.items()): row for row in previous.get(table, [])}
            curr_rows = {tuple(row.items()): row for row in current.get(table, [])}

            added = [row for key, row in curr_rows.items() if key not in prev_rows]
            removed = [row for key, row in prev_rows.items() if key not in curr_rows]

            if added or removed:
                diff[table] = {"added": added, "removed": removed}

        return diff

    def finalize(self):
        """
        Finalize snapshots, either as full state or as diffs between consecutive snapshots.
        """
        if not self.diff_snapshots:
            return {name: state for name, state in self.snapshots}

        diffs = {}
        for i in range(1, len(self.snapshots)):
            prev_name, prev_state = self.snapshots[i - 1]
            curr_name, curr_state = self.snapshots[i]
            diffs[f"{curr_name}_diff"] = self.calculate_diff(prev_state, curr_state)

        return diffs
