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
        self._load()

    # ---------------- VALIDATION ----------------

    def _validate_row(self, row, skip_pk_check=False):
        for col, col_type in self.columns.items():
            if col not in row:
                raise ValueError(f"Missing column: {col}")

            if not isinstance(row[col], SUPPORTED_TYPES[col_type]):
                raise TypeError(f"Column '{col}' expects {col_type}")

        for key in row:
            if key not in self.columns:
                raise ValueError(f"Unknown column: {key}")

        if self.primary_key and not skip_pk_check:
            pk_val = row[self.primary_key]
            for r in self.rows:
                if r[self.primary_key] == pk_val:
                    raise ValueError("Primary key violation")

        for ucol in self.unique_keys:
            val = row[ucol]
            for r in self.rows:
                if r[ucol] == val:
                    raise ValueError(f"Unique constraint violation on {ucol}")

    # ---------------- INSERT ----------------

    def insert(self, row):
        self._validate_row(row)
        self.rows.append(row)
        self._save()

    # ---------------- SELECT (WHERE) ----------------

    def filter_rows(self, column, op, value):
        result = []
        for row in self.rows:
            if op == "=" and row[column] == value:
                result.append(row)
            elif op == ">" and row[column] > value:
                result.append(row)
        return result

    # ---------------- UPDATE (WHERE) ----------------

    def update_where(self, set_column, set_value, where_column, where_value):
        updated = 0

        for row in self.rows:
            if row[where_column] == where_value:
                new_row = row.copy()
                new_row[set_column] = set_value

                self._validate_row(new_row, skip_pk_check=True)

                row[set_column] = set_value
                updated += 1

        if updated:
            self._save()

        return updated

    # ---------------- DELETE (WHERE) ----------------

    def delete_where(self, where_column, where_value):
        before = len(self.rows)
        self.rows = [r for r in self.rows if r[where_column] != where_value]
        deleted = before - len(self.rows)

        if deleted:
            self._save()

        return deleted

    # ---------------- STORAGE ----------------

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
        with open(self.file_path) as f:
            data = json.load(f)
            self.rows = data.get("rows", [])
