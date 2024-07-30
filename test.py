print("Hello!")

import psycopg2
import pandas as pd

db1_config = {
    'dbname': 'fiscozen',
    'user': 'administrator',
    'password': '2Ky6X4!BxFqtue6==',
    'host': 'data-platformf0ae860.cvq86cwow7on.eu-west-1.rds.amazonaws.com',
    'port': '5432'
}

db2_config = {
    'dbname': 'postgres',
    'user': 'administrator',
    'password': 'mxr4C9=hF!4KtE9Qi=',
    'host': 'data-platform-aurora-instance-1.cvq86cwow7on.eu-west-1.rds.amazonaws.com',
    'port': '5432'
}

conn1 = psycopg2.connect(**db1_config)
cursor1 = conn1.cursor()

conn2 = psycopg2.connect(**db2_config)
cursor2 = conn2.cursor()

cursor1.execute("SELECT * FROM fiscozen_user limit 10")
rows1 = cursor1.fetchall()
columns1 = [desc[0] for desc in cursor1.description]
df1 = pd.DataFrame(rows1, columns=columns1)
print(df1)

#cursor2.execute("SELECT * FROM ??")
#rows2 = cursor2.fetchall()
#for row in rows2:
#    print(row)
    
cursor1.close()
conn1.close()

cursor2.close()
conn2.close()

print("Finish")
