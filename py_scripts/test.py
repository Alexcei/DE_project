import sqlite3
from py_scripts.loading import txt_to_sql, xlsx_to_sql

path = './'
conn = sqlite3.connect('../sber.db')
cursor = conn.cursor()


def create_blacklist_table(cursor):
    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_PASSPORT_BLACKLIST_HIST (
            id integer primary key,
            passport_num varchar(32),
            entry_dt date,
            create_dt datetime default current_timestamp,
            update_dt datetime default NULL
            
           -- effective_from datetime default current_timestamp,
           -- effective_to datetime default (datetime('2999-12-31 23:59:59')),
           -- deleted_flg integer default 0
        );
    ''')

    cursor.execute('''
        CREATE VIEW if not exists V_PASSPORT_BLACKLIST_HIST as
            select
                id,
                passport_num,
                entry_dt
        from DWH_DIM_PASSPORT_BLACKLIST_HIST
        --where deleted_flg = 0;
    ''')


i = 0
for line in cursor.execute('''select distinct fio, event_type from REP_FRAUD;''').fetchall():
    print(line)
    i += 1

print(i)

def f():
    return cursor.execute('''
        select distinct
        t1.trans_date,
        t1.trans_id,
        t1.card_num,
        t1.oper_type,
        t1.oper_result,
        t1.amt,
        t5.passport_num,
        t5.passport_valid_to,
        t4.valid_to,
        t4.client,
        t2.terminal_city,
        t5.last_name,
        t5.first_name,
        t5.patronymic,
        t5.phone
        from DWH_DIM_TRANSACTIONS t1
        left join DWH_DIM_TERMINALS t2 on t1.terminal = t2.terminal_id
        left join cards t3 on t1.card_num = t3.card_num
        left join accounts t4 on t3.account = t4.account
        left join clients t5 on t4.client = t5.client_id
        where (select strftime('%s', max(trans_date)) from DWH_DIM_TRANSACTIONS) - strftime('%s', trans_date) < 3600 * 24
''').fetchall()


def f_():
    return cursor.execute('''
        select 
            trans_date,
            (select max(trans_date) from DWH_DIM_TRANSACTIONS) as max_d
        from DWH_DIM_TRANSACTIONS
        where (select strftime('%s', max(trans_date)) from DWH_DIM_TRANSACTIONS) - strftime('%s', trans_date) < 3600 * 24
    ''').fetchall()


# i = 0
# for line in f():
#     print(line)
#     i += 1
#
# print(i)
