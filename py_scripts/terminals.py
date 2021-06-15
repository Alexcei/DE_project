from py_scripts.utils import print_error


@print_error
def create_terminals_table(cursor):
    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_TERMINALS_HIST(
        id integer primary key autoincrement,
        terminal_id varchar(16),
        terminal_type varchar(16),
        terminal_city varchar(32),
        terminal_address varchar(128),
        start_dttm datetime default current_timestamp,
        end_dttm datetime default (datetime('2999-12-31 23:59:59')))
    ''')

    cursor.execute('''
        CREATE VIEW if not exists V_TERMINALS as
        select
        id,
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address
        from DWH_DIM_TERMINALS_HIST
        where current_timestamp between start_dttm and end_dttm;
    ''')


@print_error
def create_terminals_new(cursor):
    cursor.execute('''
        create table terminals_01 as
        select
        t1.*
        from stg_terminals t1
        left join V_TERMINALS t2
        on t1.terminal_id = t2.terminal_id
        where t2.terminal_id is null;
    ''')


@print_error
def create_terminals_delete(cursor):
    cursor.execute('''
        create table terminals_02 as
        select
        t1.terminal_id
        from V_TERMINALS t1
        left join stg_terminals t2
        on t1.terminal_id = t2.terminal_id
        where t2.terminal_id is null;
    ''')


@print_error
def create_terminals_changed(cursor):
    cursor.execute('''
        create table terminals_03 as
        select
        t1.*
        from stg_terminals t1
        inner join V_TERMINALS t2
        on t1.terminal_id = t2.terminal_id
        and (t1.terminal_type <> t2.terminal_type
        or t1.terminal_city <> t2.terminal_city
        or t1.terminal_address <> t2.terminal_address
        )
    ''')


@print_error
def update_terminals_table(conn):
    conn.cursor().execute('''
        UPDATE DWH_DIM_TERMINALS_HIST
        set end_dttm = datetime('now', '-1 second')
        where terminal_id in (select terminal_id from terminals_02)
        and end_dttm = datetime('2999-12-31 23:59:59')
    ''')

    conn.cursor().execute('''
        UPDATE DWH_DIM_TERMINALS_HIST
        set end_dttm = datetime('now', '-1 second')
        where terminal_id in (select terminal_id from terminals_03)
        and end_dttm = datetime('2999-12-31 23:59:59')
    ''')

    conn.cursor().execute('''
        INSERT INTO DWH_DIM_TERMINALS_HIST(
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address
        )select
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address
        from terminals_01
    ''')

    conn.cursor().execute('''
        INSERT INTO DWH_DIM_TERMINALS_HIST(
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address
        )select
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address
        from terminals_03
    ''')
    conn.commit()
