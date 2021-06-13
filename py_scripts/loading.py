import os
import os.path
import pandas as pd
from py_scripts.utils import print_error


@print_error
def create_table_accounts_cards_client(cursor):
    with open('ddl_dml.sql', 'r') as df:
        cursor.executescript(df.read())


@print_error
def txt_to_sql(path, conn, name_table, name_table_db):
    res = []

    for file in os.listdir(path):
        if file.split('_')[0] == name_table:
            res.append(pd.read_csv(path + file, sep=";"))
            os.rename(path + file, path + 'archive/' + file + '.backup')

    if res:
        pd.concat(res).to_sql(name_table_db, con=conn, if_exists='replace', index=False)


@print_error
def xlsx_to_sql(path, conn, name_table, name_table_db):
    res = []

    for file in os.listdir(path):
        if file.split('_')[0] == name_table:
            res.append(pd.read_excel(path + file))
            os.rename(path + file, path + 'archive/' + file + '.backup')

    if res:
        pd.concat(res).to_sql(name_table_db, con=conn, if_exists='replace', index=False)
