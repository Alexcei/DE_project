import os
import os.path
import sqlite3
import pandas as pd


conn = sqlite3.connect('sber.db')
cursor = conn.cursor()

# with open('ddl_dml.sql', 'r') as df:
#     cursor.executescript(df.read())

path = 'data/'
if not os.path.exists(path + 'archive'):
    os.makedirs(path + 'archive')


def get_stg(path, name_table):
    res = []

    for file in os.listdir(path):
        if file.split('_')[0] == name_table:
            res.append(pd.read_csv(path + file, sep=";"))
           # os.rename(path + file, path + 'archive/' + file + '.backup')

    return pd.concat(res)


transactions = get_stg(path, 'transactions')
transactions.to_sql('transactions', con=conn, if_exists='replace', index=False)

res = cursor.execute('select * from transactions').fetchall()
print(res)
