#try.py
import pyodbc

# Define connection string
conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=master;Trusted_Connection=yes;"

try:
    # Establish connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Test query
    cursor.execute("SELECT @@VERSION;")
    row = cursor.fetchone()
    
    print("Connected Successfully!")
    print("SQL Server Version:", row[0])

    # Close connection
    conn.close()

except Exception as e:
    print("Connection Failed:", str(e))