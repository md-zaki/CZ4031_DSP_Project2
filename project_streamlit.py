import subprocess

print("Enter the name of the database that you want to connect:")
db = input()

print("Enter the database server address e.g., localhost or an IP address:")
host = input()

print("Enter the username used to authenticate:")
user = input()

print("Enter the password used to authenticate:")
pwd = input()
subprocess.run(["streamlit","run","interface_streamlit.py", "--", "--db", db, '--host', host, '--user', user, '--pwd', pwd])