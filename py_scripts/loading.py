import os
import os.path
import pandas as pd
from py_scripts.utils import print_error, to_log


def get_files(path):
    catalog = os.listdir(path)
    files = []

    for file in catalog:
        if file.split('_')[0] == 'transactions':
            files.append(file)

    if len(files) == 0:
        to_log('Not files for loading!', error='Error ')
        return None

    res = []
    for file in sorted(files):
        postfix = file.split('_')[-1].split('.')[0]
        blacklist = 'passport_blacklist_' + postfix + '.xlsx'
        terminals = 'terminals_' + postfix + '.xlsx'
        transactions = 'transactions_' + postfix + '.txt'
        if blacklist not in catalog:
            to_log(f'Not file {blacklist} ', error='Error ')
            return None

        if terminals not in catalog:
            to_log(f'Not file {terminals} ', error='Error ')
            return None

        if transactions not in catalog:
            to_log(f'Not file {transactions} ', error='Error ')
            return None
        res.append([blacklist, terminals, transactions])

    return res


@print_error
def create_table_accounts_cards_client(cursor):
    with open('ddl_dml.sql', 'r') as df:
        cursor.executescript(df.read())


@print_error
def txt_to_sql(path, conn, file, name_table_db):
    df = pd.read_csv(path + file, sep=";")
    df.to_sql(name_table_db, con=conn, if_exists='replace', index=False)
    os.rename(path + file, path + 'archive/' + file + '.backup')


@print_error
def xlsx_to_sql(path, conn, file, name_table_db):
    df = pd.read_excel(path + file)
    df.to_sql(name_table_db, con=conn, if_exists='replace', index=False)
    os.rename(path + file, path + 'archive/' + file + '.backup')


