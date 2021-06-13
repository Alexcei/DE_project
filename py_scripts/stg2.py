from py_scripts.utils import print_error


@print_error
def create_blacklist_table(cursor):
    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_PASSPORT_BLACKLIST_HIST (
            id integer primary key,
            passport_num varchar(32),
            entry_dt date,
            effective_from datetime default current_timestamp,
            effective_to datetime default (datetime('2999-12-31 23:59:59')),
            deleted_flg integer default 0
        );
    ''')

    cursor.execute('''
        CREATE VIEW if not exists V_PASSPORT_BLACKLIST_HIST as
            select
                id,
                passport_num,
                entry_dt
        from DWH_DIM_PASSPORT_BLACKLIST_HIST
        where deleted_flg = 0;
    ''')


@print_error
def update_blacklist(conn):
    cursor = conn.cursor()

    cursor.execute('''
        insert into DWH_DIM_PASSPORT_BLACKLIST_HIST (
            passport_num, 
            entry_dt)
            select
                passport, 
                date
            from stg_blacklist;
    ''')

    cursor.execute('''
        insert into DWH_DIM_PASSPORT_BLACKLIST_HIST (
            passport_num, 
            entry_dt)
            select
                passport, 
                date
            from stg_blacklist;
    ''')
    conn.commit()


@print_error
def create_transactions_table(cursor):
    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_TRANSACTIONS (
            trans_id varchar(16),
            trans_date date,
            card_num varchar(32),
            oper_type varchar(16),
            amt decimal(10,2),
            oper_result varchar(16),
            terminal varchar(32),
            create_dt datetime default current_timestamp
            
            
                        effective_from datetime default current_timestamp,
            effective_to datetime default (datetime('2999-12-31 23:59:59')),
            deleted_flg integer default 0
        );
    ''')

    cursor.execute('''
        CREATE VIEW if not exists V_TRANSACTIONS as
            select
                trans_id,
                trans_date,
                card_num,
                oper_type,
                amt,
                oper_result,
                terminal
            from DWH_DIM_TRANSACTIONS
        where deleted_flg = 0;
    ''')




@print_error
def insert_transactions(conn):
    conn.cursor().execute('''
        insert into DWH_DIM_TRANSACTIONS (
            passport_num, 
            entry_dt)
            select
                passport_num, 
                entry_dt
            from stg_blacklist;
    ''')
    conn.commit()
