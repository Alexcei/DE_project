import os
import os.path
import sqlite3
import pandas as pd


path = 'data/'

if not os.path.exists(path + 'archive'):
    os.makedirs(path + 'archive')


def get_stg(path, name_table):
    res = []

    for file in os.listdir(path):
        if file.split('_')[0] == name_table:
            res.append(pd.read_csv(path + file, sep=";"))
            os.rename(path + file, path + 'archive/' + file + '.backup')

    return pd.concat(res)

transactions = get_stg(path, 'transactions')


