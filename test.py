import os
import os.path
import sqlite3
import pandas as pd


conn = sqlite3.connect('sber.db')
cursor = conn.cursor()

path = 'data/'


def xlsx_to_sql(path, conn, name_table, name_table_db):
    res = []

    for file in os.listdir(path):
        if file.split('_')[0] == name_table:
            res.append(pd.read_excel(path + file))
            os.rename(path + file, path + 'archive/' + file + '.backup')

    if res:
        pd.concat(res).to_sql(name_table_db, con=conn, if_exists='replace', index=False)


xlsx_to_sql(path, conn, 'passport', 'stg_blacklist')
xlsx_to_sql(path, conn, 'terminals', 'stg_terminals')

res = cursor.execute('select * from stg_terminals').fetchall()
print(res)

