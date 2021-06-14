from py_scripts.utils import print_error


@print_error
def create_passport_blacklist(cursor):
    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_PASSPORT_BLACKLIST (
            passport_num varchar(32),
            entry_dt date,
            create_dt datetime default current_timestamp
        );
    ''')

    cursor.execute('''
        CREATE VIEW if not exists V_PASSPORT_BLACKLIST as
            select
                passport_num,
                entry_dt
        from DWH_DIM_PASSPORT_BLACKLIST;
    ''')


@print_error
def insert_passport_blacklist(conn):
    conn.cursor().execute('''          
        insert into DWH_DIM_PASSPORT_BLACKLIST 
            (passport_num,
            entry_dt) 
            
            select
                passport,
                date
            from (  
                select
                    t1.date,
                    t1.passport,
                    t2.passport_num
                from stg_blacklist t1
                left join V_PASSPORT_BLACKLIST t2
                on t1.passport = t2.passport_num
                where t2.passport_num is null
                group by passport
            )
            where passport_num is null
    ''')
    conn.commit()
