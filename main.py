#!/usr/bin/env python3

import os
import os.path
import sqlite3
from py_scripts.utils import to_log, dropping_tables
from py_scripts.loading import create_table_accounts_cards_client
from py_scripts.loading import txt_to_sql, xlsx_to_sql

from py_scripts.report import create_table_rep_fraud
from py_scripts.report import create_table_stg_fraud
from py_scripts.report import insert_rep_fraud


def main():
    to_log('-------------Start of work----------------')

    path = 'data/'
    conn = sqlite3.connect('sber.db')
    cursor = conn.cursor()

    to_log('Dropping temporary tables')
    dropping_tables(cursor)
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
    create_table_rep_fraud(cursor)
    create_table_stg_fraud(cursor)
    insert_rep_fraud(cursor)

    to_log('Dropping temporary tables')
    #dropping_tables(cursor)

    cursor.close()
    conn.close()

    to_log('-------------Work completed----------------\n')


if __name__ == "__main__":
    main()
