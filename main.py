#!/usr/bin/env python3

import os
import os.path
import sqlite3

from py_scripts.utils import to_log, dropping_tables
from py_scripts.loading import create_table_accounts_cards_client
from py_scripts.loading import txt_to_sql, xlsx_to_sql, get_files

from py_scripts.report import create_rep_fraud
from py_scripts.report import insert_rep_fraud_1_2#, insert_rep_fraud_2
from py_scripts.report import insert_rep_fraud_3, insert_rep_fraud_4

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

    create_table_accounts_cards_client(cursor)

    if not os.path.exists(path + 'archive'):
        os.makedirs(path + 'archive')

    files = get_files(path)
    if not files:
        cursor.close()
        conn.close()
        return

    for blacklist, terminals, transactions in files:

        to_log('Dropping temporary tables')
        dropping_tables(cursor)

        to_log('Loading source files into staging tables')
        xlsx_to_sql(path, conn, blacklist, 'stg_blacklist')
        xlsx_to_sql(path, conn, terminals, 'stg_terminals')
        txt_to_sql(path, conn, transactions, 'stg_transactions')

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
        insert_rep_fraud_1_2(conn)
        #insert_rep_fraud_2(conn)
        insert_rep_fraud_3(conn)
        insert_rep_fraud_4(conn)

        to_log('Dropping temporary tables')
        dropping_tables(cursor)

    cursor.close()
    conn.close()

    to_log('-------------Work completed----------------\n')


if __name__ == "__main__":
    main()
