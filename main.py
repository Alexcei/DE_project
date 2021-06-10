import os
import os.path
import sqlite3
from py_scripts.utils import to_log
from py_scripts.loading import create_table_accounts_cards_client
from py_scripts.loading import txt_to_sql, xlsx_to_sql


def main():
    to_log('-------------Start of work----------------')

    path = 'data/'
    conn = sqlite3.connect('sber.db')
    cursor = conn.cursor()

    create_table_accounts_cards_client(cursor)

    if not os.path.exists(path + 'archive'):
        os.makedirs(path + 'archive')

    to_log('Loading source files into staging tables')
    txt_to_sql(path, conn, 'transactions', 'stg_transactions')
    xlsx_to_sql(path, conn, 'passport', 'stg_blacklist')
    xlsx_to_sql(path, conn, 'terminals', 'stg_terminals')

    to_log('creating fact tables')
    # pass

    to_log('creating dimension tables')
    # pass

    to_log('Creat Report')
    # pass

    to_log('dropping temporary tables')
    # pass

    cursor.close()
    conn.close()

    to_log('-------------Work completed----------------')


if __name__ == "__main__":
    main()
