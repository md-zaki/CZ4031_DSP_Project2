import subprocess
from psycopg2 import connect, OperationalError

dbDefault = 'postgres'
hostDefault = 'localhost'
userDefault = 'postgres'

# Use a test connection to validate input for postgresql authentication
test_conn = None
print("============================== Connecting to PostgreSQL ==================================")
while not test_conn:
    print("Enter the database name that you want to connect to (dbname): (Leave empty for default = postgres)")
    db = input()
    if db == '':
        db = dbDefault

    print("Enter the database server address e.g., localhost or an IP address (host): (Leave empty for default = localhost)" )
    host = input()
    if host == '':
        host = hostDefault

    print("Enter the username used to authenticate to postgresql (username): (Leave empty for default = postgres)")
    user = input()
    if user == '':
        user = userDefault

    print("Enter the password used to authenticate to postgresql (password):")
    pwd = input()

    try:
        test_conn = connect(database=db, 
                            host=host, 
                            user=user, 
                            password=pwd, 
                            port="5432")
    except OperationalError as err:
        print(err)
        print("Please provide correct database connection parameters")
        test_conn = None

# Authentication successful, can close test connection
test_conn.close()
test_conn = None

# Run app
subprocess.run(["streamlit","run","interface.py", "--", "--db", db, '--host', host, '--user', user, '--pwd', pwd])