import sqlite3

path = './'
conn = sqlite3.connect('../sber.db')
cursor = conn.cursor()


def create_transactions_table(cursor):
    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_TRANSACTIONS (
            trans_id varchar(16),
            trans_date date,
            card_num varchar(128),
            oper_type varchar(16),
            amt decimal(10,2),
            oper_result varchar(16),
            terminal varchar(32),
            create_dt datetime default current_timestamp
        );
    ''')


def insert_transactions_table(conn):
    conn.cursor().execute('''
        insert into DWH_DIM_TRANSACTIONS (
            trans_id,
            trans_date,
            card_num,
            oper_type,
            amt,
            oper_result,
            terminal) 
                     
            select
                transaction_id,
                transaction_date,
                amount,
                card_num,
                oper_type,
                oper_result,
                terminal
            from stg_transactions
    ''')
    conn.commit()


cursor.execute('drop table if exists DWH_DIM_TRANSACTIONS')

create_transactions_table(cursor)
insert_transactions_table(conn)


def show():
    return cursor.execute('''
        select * from DWH_DIM_TRANSACTIONS
    ''').fetchall()


i = 0
for line in show():
    i += 1

print(i)
