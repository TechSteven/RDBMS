# RDBMS
# John Ronnie MiniRDBMS

John Ronnie MiniRDBMS is a lightweight, file-based relational database management system (RDBMS) implemented in Python. It supports SQL-like commands and allows you to create tables, insert, update, delete, and query data. The system also includes a web-based interactive interface built with Flask for live SQL execution and result visualization.

# Table of Contents

1. Project Overview
2. Features
3. Phases Overview
4. Setup Instructions
5. Usage
6. Web UI
7. Example Commands
8. Directory Structure
9. Technologies Used
10. License


# Project Overview

MiniRDBMS is designed as a learning and demo project to simulate core database functionalities without using an external RDBMS engine. It uses JSON files as persistent storage and supports a subset of SQL commands including CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, JOIN, WHERE, and indexing.

This project is also equipped with an interactive Flask-based web interface, allowing users to execute SQL commands in a clean, user-friendly environment.

# Features

1. Create tables with columns, primary keys, and unique constraints.
2. Insert, update, and delete rows.
3. Query data with SELECT, WHERE, and JOIN.
4. Create and use column indexes for faster queries.
5. Persistent storage using JSON files.
6. Interactive web UI to run queries and see live results.
7. Input validation and constraint enforcement (Primary Key & Unique).
8. Clear, reset functionality in the UI for testing and demos.

# Phases Overview
# Phase 1 – Table Creation

Define tables with columns and data types.
Support primary key constraints.

# Phase 2 – Insert

Insert rows into tables.
Automatic type conversion (INT, FLOAT, TEXT).

# Phase 3 – Select

Select specific columns or all columns using *.
Support simple WHERE conditions.

# Phase 4 – Update

Update values in rows based on conditions.

# Phase 5 – Delete

Delete rows using WHERE conditions.

# Phase 6 – Indexing

Create indexes on table columns to optimize search.

# Phase 7 – Joins

Inner joins between tables.
Support querying multiple tables in a single command.

# Phase 8 – Advanced Queries

Complex WHERE conditions with >, <, >=, <=, !=.

# Phase 9 – Testing and Validation

Thorough testing of all operations.
Ensured consistent behavior of inserts, updates, deletes, joins, and selects.

# Phase 10 – Web Interface Demo

Interactive Flask-based web interface.
Input area for SQL commands.
Live result display and query reset.
Styled with CSS for modern and appealing UI.


# Setup Instructions

1. Clone the Repository
git clone https://github.com/TechSteven/RDBMS
cd mini_rdbms

2. Create a Virtual Environment 
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

3. Install Dependencies
pip install -r requirements.txt


Requirements include:

Flask

Werkzeug

4. Run the REPL (Command-Line Interface)
python3 repl.py


This will start the text-based SQL interface.

5. Run the Web UI
python3 app.py


Navigate to http://127.0.0.1:5000 in your browser.

Enter SQL commands and see results interactively.

6. Reset Data

To start fresh:

rm -f data/*.json

# Usage
# Sample Commands
CREATE TABLE users id:INT name:TEXT PRIMARY_KEY=id UNIQUE=name
CREATE TABLE orders id:INT user_id:INT total:FLOAT PRIMARY_KEY=id

INSERT users id=1 name=Alice
INSERT users id=2 name=Bob
INSERT orders id=1 user_id=1 total=100.0
INSERT orders id=2 user_id=1 total=150.0

SELECT * FROM users
SELECT users.name, orders.total FROM users JOIN orders ON users.id=orders.user_id
UPDATE users SET name=Alice2 WHERE id=1
DELETE orders WHERE id=2

# Web UI Commands

Enter commands in the input box.

Click Run Query to execute.

Click Clear to reset input.

Results are displayed dynamically below.


# Directory Structure

mini_rdbms/
├─ data/               # JSON files for table storage
├─ rdbms/
│  ├─ __init__.py
│  ├─ executor.py      # Main executor of SQL commands
│  ├─ table.py         # Table class handling storage & queries
├─ app.py              # Flask web interface
├─ repl.py             # Command-line interface
├─ main.py             # Optional script for demo
├─ requirements.txt
├─ README.md
└─ venv/               # Virtual environment


Technologies Used

1. Python 3.12+

2. Flask (Web Interface)

3. JSON (Persistent Storage)

4. HTML, CSS, JavaScript (UI)