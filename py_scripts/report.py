from py_scripts.utils import print_error


@print_error
def create_table_rep_fraud(cursor):
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


@print_error
def create_table_stg_fraud(cursor):
    return cursor.execute('''
    select DISTINCT
        tr.trans_date event_dt ,
        cl.passport_num passport,
        cl.last_name || ' ' ||  cl.first_name || ' ' || cl.patronymic fio,
        cl.phone phone,
        '1' event_type,
    from stg_transactions tr
    inner join cards cd
        ON tr.card_num = trim(cd.card_num) AND tr.trans_date between cd.effective_from AND cd.effective_to
    ''').fetchall()


'''
-- 1. Совершение операции при просроченном или заблокированном паспорте.
INSERT INTO DEMIPT.PVVL_REP_FRAUD (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
SELECT DISTINCT
--    to_char(trans_date, 'YYYY.MM.DD HH24:MI:SS' ),
    tr.trans_date event_dt ,
    cl.passport_num passport,
    cl.last_name || ' ' ||  cl.first_name || ' ' || cl.patronymic fio,
    cl.phone phone,
    '1' event_type,
    sysdate report_dt
FROM DEMIPT.PVVL_DWH_FCT_TRANSACTIONS tr
INNER JOIN DEMIPT.PVVL_DWH_DIM_CARDS_HIST cd
    ON tr.card_num = trim(cd.card_num) AND tr.trans_date between cd.effective_from AND cd.effective_to
INNER JOIN PVVL_DWH_DIM_ACCOUNTS_HIST ac 
    ON cd.account = ac.account AND tr.trans_date between ac.effective_from AND ac.effective_to
INNER JOIN PVVL_DWH_DIM_CLIENTS_HIST cl
    ON ac.client = cl.client_id AND tr.trans_date between cl.effective_from AND cl.effective_to
WHERE tr.trans_date > cl.passport_valid_to
OR cl.PASSPORT_NUM in (select PASSPORT_NUM from DEMIPT.PVVL_DWH_FCT_PSSPRT_BLACKLIST);
'''

@print_error
def create_table_stg_fraud_(cursor):
    cursor.execute('''
        create table STG_FRAUD as
            select * from (
            select distinct
                date as event_dt,
                passport as passport_num,
                last_name||' '||first_name||' '||patronymic as fio,
                phone,
                case
                    when passport_valid_to<date
                        then 'passport_block'
                    when account_valid_to<date
                        then 'account_block'
                    when
                        city <> lag(city)over(partition by card order by trans_id)
                        and strftime('%s',date)/60-strftime('%s',lag(date)over(partition by card order by trans_id))/60<60
                        then 'location_fraud'
                    when 
                        oper_result='Успешно'
                        and lag(oper_result,1)over(partition by card order by trans_id)='Отказ'
                        and lag(oper_result,2)over(partition by card order by trans_id)='Отказ'
                        and amount<lag(amount,1)over(partition by card order by trans_id)
                        and lag(amount,1)over(partition by card order by trans_id)<lag(amount,2)over(partition by card order by trans_id)						
                        and strftime('%s',date)/60-strftime('%s',lag(date)over(partition by card order by trans_id))/60<20
                        then 'selection_fraud'
                    end as event_type,
                    date(current_timestamp) as report_dt
            from stg_transactions
            where passport not in (select passport_num from stg_blacklist)) t1
            where event_type is not null 
    ''')


@ print_error
def insert_rep_fraud(cursor):
    cursor.execute('''
        insert into REP_FRAUD
            (event_dt, 
            passport,
            fio,
            phone,
            event_type,
            report_dt)
            select
                event_dt,
                passport_num,
                fio,
                phone,
                event_type,
                report_dt
            from STG_FRAUD
            where event_dt in (select max(event_dt) from STG_FRAUD group by passport_num)
    ''')
