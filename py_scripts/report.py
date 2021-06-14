from py_scripts.utils import print_error


@print_error
def create_rep_fraud(cursor):
    cursor.execute('''
        CREATE table if not exists REP_FRAUD(
                event_dt datetime, 
                passport varchar(64),
                fio varchar(128),
                phone varchar(64),
                event_type varchar(64),
                report_dt datetime default current_timestamp
            )
    ''')


@print_error
def insert_rep_fraud_1_2(conn):
    conn.cursor().execute('''
        with tmp as (    
            select distinct
                t1.trans_date,
                t1.trans_id,
                t4.valid_to,
                t5.passport_num,
                t5.passport_valid_to,
                t5.last_name,
                t5.first_name,
                t5.patronymic,
                t5.phone
            from DWH_DIM_TRANSACTIONS t1
            left join cards t3 on t1.card_num = t3.card_num
            left join accounts t4 on t3.account = t4.account
            left join clients t5 on t4.client = t5.client_id
            where (select strftime('%s', max(trans_date)) from DWH_DIM_TRANSACTIONS) -
                strftime('%s', trans_date) < 3600 * 24
            )
            insert into REP_FRAUD (
                event_dt,
                passport,
                fio,
                phone,
                event_type
            ) 
            select
                min(event_dt) as event_dt,
                passport_num,
                fio,
                phone,
                event_type   
            from (
            select distinct
                trans_date as event_dt,
                passport_num as passport_num,
                last_name || ' ' || first_name || ' ' || patronymic as fio,
                phone,
                case
                    -- 1 Совершение операции при просроченном или заблокированном паспорте. 
                    when passport_valid_to < trans_date
                        then 'block_passport'
                    -- 2 Совершение операции при недействующем договоре.
                    when valid_to < trans_date
                        then 'account_no_valid'
                end as event_type
            from tmp
            )
            where event_type is not null 
            and passport_num not in (select passport_num from DWH_DIM_PASSPORT_BLACKLIST)
            group by passport_num, fio, phone, event_type
    ''')
    conn.commit()


@print_error
def insert_rep_fraud_3(conn):
    conn.cursor().execute('''
        with tmp as (    
            select distinct
                t1.trans_date,
                t1.trans_id,
                t1.card_num,
                t1.oper_type,
                t1.oper_result,
                t1.amt,
                t5.passport_num,
                t5.passport_valid_to,
                t4.valid_to,
                t4.client,
                t2.terminal_city,
                t5.last_name,
                t5.first_name,
                t5.patronymic,
                t5.phone
            from DWH_DIM_TRANSACTIONS t1
            left join DWH_DIM_TERMINALS t2 on t1.terminal = t2.terminal_id
            left join cards t3 on t1.card_num = t3.card_num
            left join accounts t4 on t3.account = t4.account
            left join clients t5 on t4.client = t5.client_id
            where (select strftime('%s', max(trans_date)) from DWH_DIM_TRANSACTIONS) -
                strftime('%s', trans_date) < 3600 * 25
            )
            insert into REP_FRAUD (
                event_dt,
                passport,
                fio,
                phone,
                event_type
            ) 
            select
                min(event_dt) as event_dt,
                passport_num,
                fio,
                phone,
                event_type
            from (
            select distinct
                trans_date as event_dt,
                passport_num as passport_num,
                last_name || ' ' || first_name || ' ' || patronymic as fio,
                phone,
                case
                    -- 3 Совершение операций в разных городах в течение одного часа.
                    when terminal_city != lag(terminal_city) over (partition by card_num order by trans_date) 
                        and strftime('%s', trans_date) / 60 -
                        strftime('%s', lag(trans_date) over (partition by card_num order by trans_id)) / 60 <= 60
                        then 'different_cities'
                end as event_type
            from tmp
            )
            where event_type is not null 
            and passport_num not in (select passport_num from DWH_DIM_PASSPORT_BLACKLIST)
            group by passport_num, fio, phone, event_type
    ''')
    conn.commit()


@print_error
def insert_rep_fraud_4(conn):
    conn.cursor().execute('''
        with tmp as (    
            select distinct
                t1.trans_date,
                t1.trans_id,
                t1.card_num,
                t1.oper_type,
                t1.oper_result,
                t1.amt,
                t5.passport_num,
                t5.passport_valid_to,
                t4.valid_to,
                t4.client,
                t2.terminal_city,
                t5.last_name,
                t5.first_name,
                t5.patronymic,
                t5.phone
            from DWH_DIM_TRANSACTIONS t1
            left join DWH_DIM_TERMINALS t2 on t1.terminal = t2.terminal_id
            left join cards t3 on t1.card_num = t3.card_num
            left join accounts t4 on t3.account = t4.account
            left join clients t5 on t4.client = t5.client_id
            where (select strftime('%s', max(trans_date)) from DWH_DIM_TRANSACTIONS) -
                strftime('%s', trans_date) < 3600 * 24 + 60 * 20
            )
            insert into REP_FRAUD (
                event_dt,
                passport,
                fio,
                phone,
                event_type
            ) 
            select
                min(event_dt) as event_dt,
                passport_num,
                fio,
                phone,
                event_type
            from (
            select distinct
                trans_date as event_dt,
                passport_num as passport_num,
                last_name || ' ' || first_name || ' ' || patronymic as fio,
                phone,
                case
                    -- 4 Попытка подбора суммы.
                    when 
                        oper_result = 'SUCCESS'
                        and lag(oper_result, 1) over (partition by card_num order by trans_date) = 'REJECT'
                        and lag(oper_result, 2) over (partition by card_num order by trans_date) = 'REJECT'
                        and amt < lag(amt, 1) over (partition by card_num order by trans_date)
                        and lag(amt, 1) over (partition by card_num order by trans_date) <
                        lag(amt, 2) over (partition by card_num order by trans_date)
                        and strftime('%s', trans_date) / 60 -
                        strftime('%s', lag(trans_date, 2) over (partition by card_num order by trans_date)) / 60 <= 20
                        and oper_type = lag(oper_type, 1) over (partition by card_num order by trans_date)
                        and oper_type = lag(oper_type, 2) over (partition by card_num order by trans_date)
                        then 'select_amount'
                end as event_type
            from tmp
            )
            where event_type is not null 
            and passport_num not in (select passport_num from DWH_DIM_PASSPORT_BLACKLIST)
            group by passport_num, fio, phone, event_type
    ''')
    conn.commit()
