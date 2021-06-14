from py_scripts.utils import print_error


@print_error
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

    cursor.execute('''
        CREATE VIEW if not exists V_TERMINALS as
            select
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address
        from DWH_DIM_TERMINALS;
    ''')


@print_error
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
