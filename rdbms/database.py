from .table import Table

class Database:
    def __init__(self):
        self.tables = {}

    def create_table(self, name, columns, primary_key=None, unique_keys=None):
        if name in self.tables:
            raise ValueError(f"Table '{name}' already exists")
        self.tables[name] = Table(name, columns, primary_key, unique_keys)

    def get_table(self, name):
        if name not in self.tables:
            raise ValueError(f"Table '{name}' does not exist")
        return self.tables[name]

    # ----------------- JOIN -----------------
    def join(self, left_table_name, right_table_name, left_column, right_column):
        left_table = self.get_table(left_table_name)
        right_table = self.get_table(right_table_name)

        result = []
        for l_row in left_table.select_all():
            for r_row in right_table.select_all():
                if l_row[left_column] == r_row[right_column]:
                    combined = {}
                    for k, v in l_row.items():
                        combined[f"{left_table_name}.{k}"] = v
                    for k, v in r_row.items():
                        combined[f"{right_table_name}.{k}"] = v
                    result.append(combined)
        return result
