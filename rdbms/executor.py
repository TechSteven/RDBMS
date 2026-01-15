# executor.py
from .table import Table

class Executor:
    def __init__(self):
        self.tables = {}

    def execute(self, cmd):
        tokens = cmd.strip().split()
        if not tokens:
            return None
        action = tokens[0].upper()

        if action == "CREATE":
            if tokens[1].upper() == "TABLE":
                return self._create_table(tokens[2:])
            elif tokens[1].upper() == "INDEX":
                return self._create_index(tokens[2:])
        elif action == "INSERT":
            return self._insert(tokens[1:])
        elif action == "SELECT":
            return self._select(tokens)
        elif action == "UPDATE":
            return self._update(tokens[1:])
        elif action == "DELETE":
            return self._delete(tokens[1:])
        else:
            raise ValueError(f"Unknown command: {cmd}")

    def _create_table(self, tokens):
        name = tokens[0]
        columns = {}
        primary_key = None
        unique = []

        for token in tokens[1:]:
            if ":" in token:
                col, typ = token.split(":")
                columns[col] = typ
            elif token.startswith("PRIMARY_KEY="):
                primary_key = token.split("=")[1]
            elif token.startswith("UNIQUE="):
                unique = token.split("=")[1].split(",")

        table = Table(name, columns, primary_key, unique)
        self.tables[name] = table
        return f"Table '{name}' created."

    def _create_index(self, tokens):
        if tokens[0].upper() != "ON":
            raise ValueError("Syntax: CREATE INDEX ON table(column)")
        table_name, col = tokens[1].split("(")
        col = col.rstrip(")")
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
        return self.tables[table_name].create_index(col)

    def _insert(self, tokens):
        table_name = tokens[0]
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
        row = {}
        for token in tokens[1:]:
            col, val = token.split("=")
            # convert numeric types automatically
            try:
                if '.' in val:
                    val = float(val)
                else:
                    val = int(val)
            except:
                pass
            row[col] = val
        return self.tables[table_name].insert(row)

    def _select(self, tokens):
        # SELECT col1, col2 FROM table [JOIN table2 ON ...] [WHERE ...]
        if "FROM" not in tokens:
            raise ValueError("Syntax error in SELECT")

        idx_from = tokens.index("FROM")
        cols = tokens[1:idx_from]
        columns = [c.strip(",") for c in cols] or None

        table_name = tokens[idx_from + 1]

        # Handle JOIN
        if "JOIN" in tokens:
            idx_join = tokens.index("JOIN")
            join_table_name = tokens[idx_join + 1]
            on_idx = tokens.index("ON")
            left_col, right_col = tokens[on_idx + 1].split("=")
            left_table_name, left_field = left_col.split(".")
            right_table_name, right_field = right_col.split(".")
            left_rows = self.tables[left_table_name].rows
            right_rows = self.tables[right_table_name].rows
            result = []
            for lr in left_rows:
                for rr in right_rows:
                    if lr[left_field] == rr[right_field]:
                        row = {}
                        if columns == ["*"]:
                            # include all columns from both tables
                            for c in self.tables[left_table_name].columns:
                                row[f"{left_table_name}.{c}"] = lr[c]
                            for c in self.tables[right_table_name].columns:
                                row[f"{right_table_name}.{c}"] = rr[c]
                        else:
                            for col in columns:
                                tname, cname = col.split(".")
                                if tname == left_table_name:
                                    row[col] = lr[cname]
                                else:
                                    row[col] = rr[cname]
                        result.append(row)
            return result

        # Regular select
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")

        where = None
        if "WHERE" in tokens:
            idx_where = tokens.index("WHERE")
            where = {}
            conditions = tokens[idx_where + 1:]
            for cond in conditions:
                for op in ["!=", ">=", "<=", "=", ">", "<"]:
                    if op in cond:
                        c, v = cond.split(op)
                        try:
                            if '.' in v:
                                v = float(v)
                            else:
                                v = int(v)
                        except:
                            pass
                        where[c] = (op, v)
                        break
        return self.tables[table_name].select(columns or ["*"], where)

    def _update(self, tokens):
        table_name = tokens[0]
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
        if "SET" not in tokens:
            raise ValueError("Syntax: UPDATE table SET col=val [WHERE ...]")
        idx_set = tokens.index("SET")
        if "WHERE" in tokens:
            idx_where = tokens.index("WHERE")
            set_tokens = tokens[idx_set + 1:idx_where]
            where_tokens = tokens[idx_where + 1:]
        else:
            set_tokens = tokens[idx_set + 1:]
            where_tokens = []

        set_values = {}
        for token in set_tokens:
            col, val = token.split("=")
            try:
                if '.' in val:
                    val = float(val)
                else:
                    val = int(val)
            except:
                pass
            set_values[col] = val

        where = {}
        for cond in where_tokens:
            for op in ["!=", ">=", "<=", "=", ">", "<"]:
                if op in cond:
                    c, v = cond.split(op)
                    try:
                        if '.' in v:
                            v = float(v)
                        else:
                            v = int(v)
                    except:
                        pass
                    where[c] = (op, v)
                    break

        return self.tables[table_name].update(set_values, where or None)

    def _delete(self, tokens):
        table_name = tokens[0]
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
        where = None
        if "WHERE" in tokens:
            idx_where = tokens.index("WHERE")
            where_tokens = tokens[idx_where + 1:]
            where = {}
            for cond in where_tokens:
                for op in ["!=", ">=", "<=", "=", ">", "<"]:
                    if op in cond:
                        c, v = cond.split(op)
                        try:
                            if '.' in v:
                                v = float(v)
                            else:
                                v = int(v)
                        except:
                            pass
                        where[c] = (op, v)
                        break
        return self.tables[table_name].delete(where)
