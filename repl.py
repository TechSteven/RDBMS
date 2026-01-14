from rdbms.executor import Executor

# Initialize executor
executor = Executor()

HELP_TEXT = """
MiniRDBMS REPL Commands:

CREATE TABLE table_name column1:type1 column2:type2 [PRIMARY_KEY=column] [UNIQUE=col1,col2]
INSERT table_name col1=val1 col2=val2 ...
SELECT * FROM table_name [WHERE column=value | column>value]
UPDATE table_name SET col=val WHERE column=value
DELETE FROM table_name WHERE column=value
EXIT
HELP
"""

def main():
    print("MiniRDBMS REPL. Type HELP for commands.")
    while True:
        cmd = input("> ").strip()
        if not cmd:
            continue  # skip empty lines

        if cmd.upper() == "HELP":
            print(HELP_TEXT)
            continue
        if cmd.upper() == "EXIT":
            print("Bye!")
            break

        try:
            result = executor.execute(cmd)
            if isinstance(result, list):
                for row in result:
                    print(row)
            else:
                print(result)
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
