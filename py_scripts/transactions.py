from py_scripts.utils import print_error


@print_error
def create_transactions_table(cursor):
    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_TRANSACTIONS (
            trans_id varchar(16),
            trans_date date,
            amt decimal(10,2),
            card_num varchar(128),
            oper_type varchar(16),
            oper_result varchar(16),
            terminal varchar(32),
            create_dt datetime default current_timestamp
        );
    ''')


@print_error
def insert_transactions_table(conn):
    conn.cursor().execute('''
        insert into DWH_DIM_TRANSACTIONS (
            trans_id,
            trans_date,
            amt,
            card_num,
            oper_type,
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
