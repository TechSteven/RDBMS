# table.py
import json
import os

DATA_DIR = "data"

class Table:
    def __init__(self, name, columns, primary_key=None, unique=None):
        self.name = name
        self.columns = columns  # {'col_name': 'type'}
        self.primary_key = primary_key
        self.unique = unique or []
        self.rows = []
        self.indexes = {}  # column_name -> {value: row}
        self.load()

    @property
    def file_path(self):
        return os.path.join(DATA_DIR, f"{self.name}.json")

    def load(self):
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)
        if os.path.exists(self.file_path):
            with open(self.file_path, "r") as f:
                self.rows = json.load(f)
        else:
            self.rows = []

    def save(self):
        with open(self.file_path, "w") as f:
            json.dump(self.rows, f, indent=2)

    def insert(self, row):
        # Check primary key
        if self.primary_key and any(r[self.primary_key] == row[self.primary_key] for r in self.rows):
            raise ValueError("Primary key violation")
        # Check unique constraints
        for col in self.unique:
            if any(r[col] == row[col] for r in self.rows):
                raise ValueError(f"Unique constraint violation on '{col}'")
        self.rows.append(row)
        self.update_indexes(row)
        self.save()
        return f"Row inserted into '{self.name}'."

    def update_indexes(self, row):
        for col, idx in self.indexes.items():
            idx[row[col]] = row

    def create_index(self, column):
        if column not in self.columns:
            raise ValueError(f"Column '{column}' does not exist")
        idx = {}
        for row in self.rows:
            idx[row[column]] = row
        self.indexes[column] = idx
        return f"Index on '{column}' created."

    def select(self, columns=None, where=None):
        result = self.rows
        if where:
            result = [row for row in result if self._match_where(row, where)]
        if columns is None or columns == ["*"]:
            return result
        return [{col: row[col] for col in columns} for row in result]

    def _match_where(self, row, where):
        # where is a dict of column -> (operator, value)
        for col, (op, val) in where.items():
            if op == "=" and row.get(col) != val:
                return False
            if op == "!=" and row.get(col) == val:
                return False
            if op == ">" and row.get(col) <= val:
                return False
            if op == "<" and row.get(col) >= val:
                return False
            if op == ">=" and row.get(col) < val:
                return False
            if op == "<=" and row.get(col) > val:
                return False
        return True

    def update(self, set_values, where=None):
        count = 0
        for row in self.rows:
            if not where or self._match_where(row, where):
                for col, val in set_values.items():
                    row[col] = val
                count += 1
        self.save()
        return f"Updated {count} rows in '{self.name}'."

    def delete(self, where=None):
        original_len = len(self.rows)
        self.rows = [row for row in self.rows if not (where and self._match_where(row, where))]
        deleted_count = original_len - len(self.rows)
        self.save()
        return f"Deleted {deleted_count} rows from '{self.name}'."

