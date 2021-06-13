from py_scripts.utils import print_error


@print_error
def create_passport_blacklist(cursor):
    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_PASSPORT_BLACKLIST_HIST (
            passport_num varchar(32),
            entry_dt date,
            create_dt datetime default current_timestamp
        );
    ''')


@print_error
def insert_passport_blacklist(conn):
    conn.cursor().execute('''          
        insert into DWH_DIM_PASSPORT_BLACKLIST_HIST 
            (passport_num,
            entry_dt) 
                     
            select
                t1.date,
                t1.passport
            from stg_blacklist t1
            left join DWH_DIM_PASSPORT_BLACKLIST_HIST t2
            on t1.passport = t2.passport_num
            where t2.passport_num is null
            group by passport
    ''')
    conn.commit()
