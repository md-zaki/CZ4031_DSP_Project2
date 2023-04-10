import subprocess

dbDefault = 'postgres'
hostDefault = 'localhost'
userDefault = 'postgres'

print("Enter the name of the database that you want to connect: (Leave empty for default = postgres)")
db = input()
if db == '':
    db = dbDefault

print("Enter the database server address e.g., localhost or an IP address: (Leave empty for default = localhost)" )
host = input()
if host == '':
    host = hostDefault

print("Enter the username used to authenticate: (Leave empty for default = postgres)")
user = input()
if user == '':
    user = userDefault

print("Enter the password used to authenticate:")
pwd = input()
subprocess.run(["streamlit","run","interface_streamlit.py", "--", "--db", db, '--host', host, '--user', user, '--pwd', pwd])