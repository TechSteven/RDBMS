import json
import os

SUPPORTED_TYPES = {
    "INT": int,
    "TEXT": str,
    "FLOAT": float
}


class Table:
    def __init__(self, name, columns, primary_key=None, unique_keys=None, data_dir="data"):
        self.name = name
        self.columns = columns
        self.primary_key = primary_key
        self.unique_keys = unique_keys or []

        self.data_dir = data_dir
        self.file_path = os.path.join(self.data_dir, f"{self.name}.json")

        self.rows = []
        self.indexes = {}  # {column: {value: [rows]}}

        self._load()

    # ----------------- Validation -----------------

    def _validate_row(self, row, skip_pk_check=False):
        for column, col_type in self.columns.items():
            if column not in row:
                raise ValueError(f"Missing column: {column}")

            expected_type = SUPPORTED_TYPES[col_type]
            if not isinstance(row[column], expected_type):
                raise TypeError(
                    f"Column '{column}' expects {col_type}, got {type(row[column]).__name__}"
                )

        for key in row:
            if key not in self.columns:
                raise ValueError(f"Unknown column: {key}")

        if self.primary_key and not skip_pk_check:
            pk_value = row[self.primary_key]
            for existing in self.rows:
                if existing[self.primary_key] == pk_value:
                    raise ValueError("Primary key violation")

        for unique_col in self.unique_keys:
            value = row[unique_col]
            for existing in self.rows:
                if existing[unique_col] == value:
                    raise ValueError(f"Unique constraint violation on '{unique_col}'")

    # ----------------- Indexing -----------------

    def create_index(self, column):
        if column not in self.columns:
            raise ValueError(f"Column '{column}' does not exist")

        index = {}
        for row in self.rows:
            value = row[column]
            index.setdefault(value, []).append(row)

        self.indexes[column] = index
        return f"Index created on {self.name}({column})"

    def _add_to_indexes(self, row):
        for column, index in self.indexes.items():
            value = row[column]
            index.setdefault(value, []).append(row)

    def _remove_from_indexes(self, row):
        for column, index in self.indexes.items():
            value = row[column]
            if value in index:
                index[value] = [r for r in index[value] if r is not row]
                if not index[value]:
                    del index[value]

    # ----------------- CRUD -----------------

    def insert(self, row):
        self._validate_row(row)
        self.rows.append(row)
        self._add_to_indexes(row)
        self._save()

    def select_all(self):
        return self.rows.copy()

    def filter_rows(self, column, op, value):
        if op == "=" and column in self.indexes:
            return self.indexes[column].get(value, []).copy()

        result = []
        for row in self.rows:
            if op == "=" and row[column] == value:
                result.append(row)
            elif op == ">" and row[column] > value:
                result.append(row)
            elif op == "<" and row[column] < value:
                result.append(row)
        return result

    def update_where(self, set_column, set_value, where_column, where_value):
        updated = 0
        for row in self.rows:
            if row[where_column] == where_value:
                self._remove_from_indexes(row)
                row[set_column] = set_value
                self._add_to_indexes(row)
                updated += 1

        self._save()
        return updated

    def delete_where(self, where_column, where_value):
        remaining = []
        deleted = 0

        for row in self.rows:
            if row[where_column] == where_value:
                self._remove_from_indexes(row)
                deleted += 1
            else:
                remaining.append(row)

        self.rows = remaining
        self._save()
        return deleted

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
