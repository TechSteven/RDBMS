from rdbms.table import Table


class Executor:
    def __init__(self):
        self.tables = {}

    def execute(self, command):
        tokens = command.strip().split()
        action = tokens[0].upper()

        if action == "CREATE" and tokens[1].upper() == "TABLE":
            return self._create_table(tokens)

        if action == "CREATE" and tokens[1].upper() == "INDEX":
            return self._create_index(tokens)

        if action == "INSERT":
            return self._insert(tokens)

        if action == "SELECT":
            return self._select(tokens)

        if action == "UPDATE":
            return self._update(tokens)

        if action == "DELETE":
            return self._delete(tokens)

        raise ValueError("Unknown command")

    # ----------------- Commands -----------------

    def _create_table(self, tokens):
        name = tokens[2]
        columns = {}
        primary_key = None
        unique_keys = []

        for token in tokens[3:]:
            if token.startswith("PRIMARY_KEY="):
                primary_key = token.split("=")[1]
            elif token.startswith("UNIQUE="):
                unique_keys = token.split("=")[1].split(",")
            else:
                col, typ = token.split(":")
                columns[col] = typ

        self.tables[name] = Table(name, columns, primary_key, unique_keys)
        return f"Table '{name}' created."

    def _create_index(self, tokens):
        table_name = tokens[3]
        column = tokens[4].strip("()")

        table = self.tables[table_name]
        return table.create_index(column)

    def _insert(self, tokens):
        table_name = tokens[1]
        table = self.tables[table_name]

        row = {}
        for token in tokens[2:]:
            col, val = token.split("=")
            if val.isdigit():
                val = int(val)
            row[col] = val

        table.insert(row)
        return f"Row inserted into '{table_name}'."

    def _select(self, tokens):
        table_name = tokens[3]
        table = self.tables[table_name]

        if "WHERE" not in tokens:
            return table.select_all()

        where_index = tokens.index("WHERE")
        condition = tokens[where_index + 1]

        if ">" in condition:
            column, value = condition.split(">")
            return table.filter_rows(column, ">", int(value))
        elif "<" in condition:
            column, value = condition.split("<")
            return table.filter_rows(column, "<", int(value))
        else:
            column, value = condition.split("=")
            return table.filter_rows(column, "=", value)

    def _update(self, tokens):
        table_name = tokens[1]
        table = self.tables[table_name]

        set_part = tokens[tokens.index("SET") + 1]
        where_part = tokens[tokens.index("WHERE") + 1]

        set_col, set_val = set_part.split("=")
        where_col, where_val = where_part.split("=")

        if set_val.isdigit():
            set_val = int(set_val)
        if where_val.isdigit():
            where_val = int(where_val)

        count = table.update_where(set_col, set_val, where_col, where_val)
        return f"Updated {count} rows in '{table_name}'."

    def _delete(self, tokens):
        table_name = tokens[2]
        table = self.tables[table_name]

        where_part = tokens[tokens.index("WHERE") + 1]
        where_col, where_val = where_part.split("=")

        if where_val.isdigit():
            where_val = int(where_val)

        count = table.delete_where(where_col, where_val)
        return f"Deleted {count} rows from '{table_name}'."
