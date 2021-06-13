import sqlite3

path = './'
conn = sqlite3.connect('../sber.db')
cursor = conn.cursor()


def create_terminals_table(cursor):
    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_TERMINALS (
            terminal_id varchar(16),
            terminal_type varchar(16),
            terminal_city varchar(32),
            terminal_address varchar(128),
            create_dt datetime default current_timestamp
        );
    ''')


def insert_terminals_table(conn):
    conn.cursor().execute('''
        insert into DWH_DIM_TERMINALS (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address) 

            select distinct
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address
            from stg_terminals
    ''')
    conn.commit()


cursor.execute('drop table if exists DWH_DIM_TERMINALS')

create_terminals_table(cursor)
insert_terminals_table(conn)


def show():
    return cursor.execute('''
        select * from DWH_DIM_TERMINALS
    ''').fetchall()


i = 0
for line in show():
    i += 1

print(i)
