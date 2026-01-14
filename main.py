from rdbms.database import Database

db = Database()

db.create_table(
    name="users",
    columns={
        "id": "INT",
        "email": "TEXT",
        "name": "TEXT"
    },
    primary_key="id",
    unique_keys=["email"]
)

db.insert("users", {"id": 1, "email": "a@test.com", "name": "Alice"})
db.insert("users", {"id": 2, "email": "b@test.com", "name": "Bob"})

print(db.select_all("users"))




# Uncomment ONE at a time to test errors

 #Duplicate primary key
#db.insert("users", {"id": 1, "email": "c@test.com", "name": "Eve"})

#Duplicate unique email
#db.insert("users", {"id": 3, "email": "a@test.com", "name": "Eve"})
