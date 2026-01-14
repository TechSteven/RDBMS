import json
import os

SUPPORTED_TYPES = {
    "INT": int,
    "TEXT": str,
    "FLOAT": float
}


class Table:
    def __init__(self, name, columns, primary_key=None, unique_keys=None, data_dir="data"):
        """
        name: str
        columns: dict -> {column_name: type_name}
        primary_key: str | None
        unique_keys: list[str]
        """
        self.name = name
        self.columns = columns
        self.primary_key = primary_key
        self.unique_keys = unique_keys or []

        self.data_dir = data_dir
        self.file_path = os.path.join(self.data_dir, f"{self.name}.json")

        self.rows = []
        self._load()

    def _validate_row(self, row, skip_pk_check=False):
        # Schema & type validation
        for column, col_type in self.columns.items():
            if column not in row:
                raise ValueError(f"Missing column: {column}")

            expected_type = SUPPORTED_TYPES[col_type]
            if not isinstance(row[column], expected_type):
                raise TypeError(
                    f"Column '{column}' expects {col_type}, "
                    f"got {type(row[column]).__name__}"
                )

        for key in row:
            if key not in self.columns:
                raise ValueError(f"Unknown column: {key}")

        # Primary key constraint
        if self.primary_key and not skip_pk_check:
            pk_value = row[self.primary_key]
            for existing in self.rows:
                if existing[self.primary_key] == pk_value:
                    raise ValueError("Primary key violation")

        # Unique constraints
        for unique_col in self.unique_keys:
            value = row[unique_col]
            for existing in self.rows:
                if existing[unique_col] == value:
                    raise ValueError(
                        f"Unique constraint violation on '{unique_col}'"
                    )

    def insert(self, row):
        self._validate_row(row)
        self.rows.append(row)
        self._save()

    def select_all(self):
        return self.rows.copy()

    def update(self, pk_value, updates):
        if not self.primary_key:
            raise ValueError("No primary key defined")

        for row in self.rows:
            if row[self.primary_key] == pk_value:
                new_row = row.copy()
                new_row.update(updates)

                # validate updated row (skip PK duplicate check)
                self._validate_row(new_row, skip_pk_check=True)

                row.update(updates)
                self._save()
                return

        raise ValueError("Row not found")

    def delete(self, pk_value):
        if not self.primary_key:
            raise ValueError("No primary key defined")

        for i, row in enumerate(self.rows):
            if row[self.primary_key] == pk_value:
                self.rows.pop(i)
                self._save()
                return

        raise ValueError("Row not found")

    # ----------------- Phase 6 methods -----------------

    def filter_rows(self, column, op, value):
        """Return rows that match a simple condition column op value"""
        result = []
        for row in self.rows:
            if op == "=" and row[column] == value:
                result.append(row)
            elif op == ">" and row[column] > value:
                result.append(row)
        return result

    def update_row(self, column, value, where_column, where_value):
        """Update rows where where_column equals where_value"""
        for row in self.rows:
            if row[where_column] == where_value:
                row[column] = value
        self._save()

    def delete_row(self, where_column, where_value):
        """Delete rows where where_column equals where_value"""
        self.rows = [r for r in self.rows if r[where_column] != where_value]
        self._save()

    # ----------------- Persistence -----------------

    def _save(self):
        os.makedirs(self.data_dir, exist_ok=True)
        with open(self.file_path, "w") as f:
            json.dump({
                "columns": self.columns,
                "primary_key": self.primary_key,
                "unique_keys": self.unique_keys,
                "rows": self.rows
            }, f, indent=2)

    def _load(self):
        if not os.path.exists(self.file_path):
            return

        with open(self.file_path, "r") as f:
            data = json.load(f)
            self.rows = data.get("rows", [])


