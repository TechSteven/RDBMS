from rdbms.database import Database

class Executor:
    def __init__(self):
        self.db = Database()

    def execute(self, command: str):
        parts = command.strip().split()
        if not parts:
            return None

        action = parts[0].upper()

        if action == "CREATE":
            if parts[1].upper() != "TABLE":
                raise ValueError("Syntax: CREATE TABLE ...")
            table_name = parts[2]
            columns = {}
            primary_key = None
            unique_keys = []

            for item in parts[3:]:
                if item.startswith("PRIMARY_KEY="):
                    primary_key = item.split("=")[1]
                elif item.startswith("UNIQUE="):
                    unique_keys = item.split("=")[1].split(",")
                else:
                    col_name, col_type = item.split(":")
                    columns[col_name] = col_type.upper()

            self.db.create_table(table_name, columns, primary_key, unique_keys)
            return f"Table '{table_name}' created."

        elif action == "INSERT":
            table_name = parts[1]
            row = {}
            for item in parts[2:]:
                col, val = item.split("=")
                val = int(val) if val.isdigit() else val
                row[col] = val
            self.db.insert(table_name, row)
            return f"Row inserted into '{table_name}'."

        elif action == "SELECT":
            if parts[1] != "*" or parts[2].upper() != "FROM":
                raise ValueError("Syntax: SELECT * FROM table_name [WHERE ...]")
            table_name = parts[3]
            table = self.db.tables[table_name]

            # Check for optional WHERE
            if len(parts) > 4 and parts[4].upper() == "WHERE":
                cond = parts[5]
                if "=" in cond:
                    col, val = cond.split("=")
                    op = "="
                elif ">" in cond:
                    col, val = cond.split(">")
                    op = ">"
                else:
                    raise ValueError("WHERE condition must be column=value or column>value")
                val = int(val) if val.isdigit() else val
                rows = table.filter_rows(col, op, val)
            else:
                rows = table.select_all()
            return rows

        elif action == "UPDATE":
            table_name = parts[1]
            if parts[2].upper() != "SET":
                raise ValueError("Syntax: UPDATE table_name SET col=val WHERE col=value")
            col, val = parts[3].split("=")
            val = int(val) if val.isdigit() else val

            if parts[4].upper() != "WHERE":
                raise ValueError("Missing WHERE clause")
            where_col, where_val = parts[5].split("=")
            where_val = int(where_val) if where_val.isdigit() else where_val

            self.db.tables[table_name].update_row(col, val, where_col, where_val)
            return f"Updated rows in '{table_name}'."

        elif action == "DELETE":
            if parts[1].upper() != "FROM":
                raise ValueError("Syntax: DELETE FROM table_name WHERE column=value")
            table_name = parts[2]
            if parts[3].upper() != "WHERE":
                raise ValueError("Missing WHERE clause")
            where_col, where_val = parts[4].split("=")
            where_val = int(where_val) if where_val.isdigit() else where_val

            self.db.tables[table_name].delete_row(where_col, where_val)
            return f"Deleted rows from '{table_name}'."

        else:
            raise ValueError(f"Unknown command: {action}")
