import sqlite3

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


def del_blacklist(cursor):
    cursor.execute('drop table if exists DWH_DIM_PASSPORT_BLACKLIST_HIST')
    cursor.execute('drop VIEW if exists V_PASSPORT_BLACKLIST_HIST;')


def update_blacklist(conn):
    cursor = conn.cursor()

    cursor.execute('''
        replace into DWH_DIM_PASSPORT_BLACKLIST_HIST t1
        (
            select
                passport, 
                max(date) as entry_dt
            from stg_blacklist
            group by passport) t2
        on t1.passport_num = t2.passport
        when matched then
            update
            set t1.entry_dt = t2.entry_dt
                t1.update_dt = current_timestamp
        when not matched then
            insert (passport_num, entry_dt)
            values (t2.passport, t2.entry_dt);
    ''')
    conn.commit()


del_blacklist(cursor)
create_blacklist_table(cursor)
update_blacklist(conn)

# tmp = cursor.execute('''
# select max(date), passport from stg_blacklist group by passport;
# ''').fetchall()
#
# for line in tmp:
#     print(line)
