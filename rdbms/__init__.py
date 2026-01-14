class Table:
    def __init__(self, name, columns, primary_key=None, unique_keys=None, data_dir="data"):
        self.name = name
        self.columns = columns
        self.primary_key = primary_key
        self.unique_keys = unique_keys or []
        self.rows = []
        self.data_dir = data_dir
        self.file_path = os.path.join(self.data_dir, f"{self.name}.json")

        self._load()
