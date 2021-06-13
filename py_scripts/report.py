import sqlite3

path = './'
conn = sqlite3.connect('../sber.db')
cursor = conn.cursor()


def create_rep_fraud(cursor):
    cursor.execute('''
        CREATE table if not exists REP_FRAUD(
                event_dt datetime, 
                passport varchar(64),
                fio varchar(64),
                phone varchar(64),
                event_type varchar(64),
                report_dt datetime
            )
    ''')


def insert_rep_fraud(conn):
    conn.cursor().execute('''	
        CREATE table if not exists DE5_DWH_DIM_REP_FRAUD (
            id integer PRIMARY KEY autoincrement,
            event_type varchar(128)
        )
        ''')

    conn.cursor().execute('''
            INSERT INTO DE5_DWH_DIM_REP_FRAUD (event_type) values	
            ('Совершение операции при просроченном или заблокированном паспорте'),
            ('Совершение операции при недействующем договоре'),
            ('Совершение операций в разных городах в течение одного часа'),
            ('Попытка подбора суммы');
        ''')

    conn.cursor().execute('''
        insert into REP_FRAUD (
            event_dt, 
            passport,
            fio,
            phone,
            event_type,
            report_dt )
            select
                t5.event_dt,
                t6.passport_num,
                t6.last_name || ' ' || t6.first_name || ' ' || t6.patronymic as fio,
                t6.phone,
                t7.event_type,
                current_timestamp
            from 
            (SELECT max(f.event_dt) as event_dt, f.client as client, f.event_type as event_type
                FROM
                (
                SELECT 
                t1.trans_date as event_dt,
                t1.client as client,
                '1' as event_type
                FROM DWH_DIM_TRANSACTIONS t1
                where t1.oper_result = 'Успешно' 
                and t1.trans_date > t1.passport_valid_to
                UNION ALL
                --2 Совершение операции при недействующем договоре
                SELECT 
                t1.trans_date as event_dt,
                t1.client as client,
                '2' as event_type
                FROM DWH_DIM_TRANSACTIONS t1
                where t1.oper_result = 'Успешно' and
                t1.trans_date > t1.account_valid_to
                UNION ALL
                --3 Совершение операций в разных городах в течение одного часа
                SELECT
                b.to_dttm as event_dt,
                b.client as client,
                '3' as event_type
                FROM
                (
                SELECT a.*,
                cast ((JulianDay(to_dttm) - JulianDay(FROM_dttm)) * 24 * 60 As Integer) as delta
                FROM (
                SELECT 
                trans_date as FROM_dttm,
                lead(trans_date) over(partition by account order by trans_date) as to_dttm,
                client,
                account,
                city,
                lead(city) over (partition by account order by trans_date) as city_2,
                oper_result,
                lead(oper_result) over (partition by account order by trans_date) as oper_result_2
                FROM DWH_DIM_TRANSACTIONS
                where oper_result = 'Успешно'
                ) a
                where to_dttm is not null
                and city <> city_2 and oper_result_2 = 'Успешно'
                and delta <= 60) b
                UNION ALL
                --4 Попытка подбора суммы
                SELECT
                d.to_dttm_3 as event_dt,
                d.client as client,
                '4' as event_type
                FROM
                (
                SELECT c.*,
                Cast ((JulianDay(to_dttm_3) - JulianDay(FROM_dttm)) * 24 * 60 As Integer) as delta
                FROM
                (
                SELECT a.*
                FROM
                (
                SELECT 
                trans_date as FROM_dttm,
                lead(trans_date, 1) over(partition by account order by trans_date) as to_dttm_1,
                lead(trans_date, 2) over(partition by account order by trans_date) as to_dttm_2,
                lead(trans_date, 3) over(partition by account order by trans_date) as to_dttm_3,
                client, 
                account,
                oper_result,
                lead(oper_result,1) over (partition by account order by trans_date) as result_1,
                lead(oper_result,2) over (partition by account order by trans_date) as result_2,
                lead(oper_result,3) over (partition by account order by trans_date) as result_3,
                amount,
                lead(amount) over (partition by account order by trans_date) as amount_1,
                lead(amount,2) over (partition by account order by trans_date) as amount_2,
                lead(amount,3) over (partition by account order by trans_date) as amount_3
                FROM DWH_DIM_TRANSACTIONS
                ) a
                where oper_result='Отказ'
                and result_1='Отказ' 
                and result_2='Отказ' 
                and result_3='Успешно'  
                and amount > amount_1 
                and amount_1 > amount_2 
                and amount_2 > amount_3
                ) c
                where delta <= 20) d
                ) f
                ---------------------------
                group by f.client, f.event_type
                ---------------------------
                ) t5
            inner join clients t6
            on t5.client = t6.client_id
            inner join DE5_DWH_DIM_REP_FRAUD t7
            on t5.event_type = t7.id
            left join REP_FRAUD t8
            on t8.event_dt = t5.event_dt 
            and t6.passport_num = t8.passport
            where t8.event_dt is null 
            and t8.passport is null 
            and t8.fio is null
            and t8.phone is null
            order by t5.event_dt
    ''')
    conn.commit()


# create_rep_fraud(cursor)
# insert_rep_fraud(conn)


def create_and_insert_(conn):
    conn.cursor().execute('''	
        CREATE table if not exists DE5_DWH_DIM_REP_FRAUD (
            id integer PRIMARY KEY autoincrement,
            event_type varchar(128)
        )
        ''')

    conn.cursor().execute('''
            INSERT INTO DE5_DWH_DIM_REP_FRAUD (event_type) values	
            ('Совершение операции при просроченном или заблокированном паспорте'),
            ('Совершение операции при недействующем договоре'),
            ('Совершение операций в разных городах в течение одного часа'),
            ('Попытка подбора суммы');
        ''')
    conn.commit()


def show():
    return cursor.execute('''
    select
        *
    from cards t3
    left join accounts t4 on t3.account = t4.account 
    left join clients t5 on t4.client = t5.client_id
    ''').fetchall()


# create_and_insert_(conn)
#
# i = 0
# for line in show():
#     print(line)

def show_():
    return cursor.execute('''
    WITH DWH_DIM_TRANSACTIONS_ AS (
    select
        *
    from DWH_DIM_TRANSACTIONS t1
    left join DWH_DIM_TERMINALS t2 on t1.terminal = t2.terminal_id
    left join cards t3 on t1.card_num = t3.card_num
    -- left join accounts t4 on t3.account = t4.account 
    -- left join clients t5 on t4.client = t5.client_id
    )
    SELECT * FROM DWH_DIM_TRANSACTIONS_;
    ''').fetchall()


for line in show():
    print(line)
print(line[0])
# stg_transactions
# stg_terminals
# stg_blacklist

# accounts
# clients