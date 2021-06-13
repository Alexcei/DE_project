#!/usr/bin/env python3

import os
import os.path
import sqlite3
from py_scripts.utils import to_log, dropping_tables
from py_scripts.loading import create_table_accounts_cards_client
from py_scripts.loading import txt_to_sql, xlsx_to_sql

from py_scripts.report import create_rep_fraud
from py_scripts.report import insert_rep_fraud

from py_scripts.backlist import create_passport_blacklist
from py_scripts.backlist import insert_passport_blacklist

from py_scripts.transactions import create_transactions_table
from py_scripts.transactions import insert_transactions_table

from py_scripts.terminals import create_terminals_table
from py_scripts.terminals import insert_terminals_table


def main():
    to_log('-------------Start of work----------------')

    path = './'
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

    to_log('Loading fact blacklist')
    create_passport_blacklist(cursor)
    insert_passport_blacklist(conn)

    to_log('Loading fact transactions')
    create_transactions_table(cursor)
    insert_transactions_table(conn)

    to_log('Loading fact terminals')
    create_terminals_table(cursor)
    insert_terminals_table(conn)

    to_log('Create Report')
    create_rep_fraud(cursor)
    insert_rep_fraud(conn)

    to_log('Dropping temporary tables')
    #dropping_tables(cursor)

    cursor.close()
    conn.close()

    to_log('-------------Work completed----------------\n')


if __name__ == "__main__":
    main()
    # path = './'
    # conn = sqlite3.connect('sber.db')
    # cursor = conn.cursor()

    # create_transactions_table(cursor)
    # insert_transactions(conn)

    # create_blacklist_table(cursor)
    # insert_blacklist(conn)
    #
    # cursor.close()
    # conn.close()
    #insert_blacklist(conn)
    # res = create_table_stg_fraud(cursor)
    # for line in res:
    #     print(line)
