DECLARE OB_START_DT DATE DEFAULT '2023-01-01'; -- start date of driver onboarding date
DECLARE OB_END_DT DATE DEFAULT '2023-12-31'; -- end date of driver onboarding date
DECLARE DLVR_END_DT DATE DEFAULT '2024-01-31'; -- end date of driver delivery date
DECLARE TOP_N INT64 DEFAULT 100; -- limit to top nth deliveries

with drvr_lst as (
  select distinct DRVR_USER_ID
  from `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.DRVR`
  where DRVR_ONBDG_TS between OB_START_DT and OB_END_DT
    and DRVR_SVC_TYPE_NM = 'REGULAR' -- excluding drivers from homeoffice and test
),

base as (
  select distinct drvr_user_id,
    SALES_ORDER_NBR,
    coalesce(PO_NBR, DLVR_TASK_ID, " ") as po_equ_nbr,
    TRIP_ID,
    FULFMT_TYPE_CD,
    ORDER_PLCD_DT,
    DLVR_CARRIER_TS,
    date_add(DLVR_CARRIER_TS, INTERVAL -1 WEEK) as rolling_week_ts,
    TRIP_ON_TM_DLVR_IND
  from `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_SECURE.DLVR_LAST_MI_DTL`
  where DLVR_CARRIER_TS is not null -- delivered
    and drvr_user_id in (select * from drvr_lst)
    and date(DLVR_CARRIER_TS) <= DLVR_END_DT
  order by DLVR_CARRIER_TS
),

dlvr as (
    select *
    from (select *, row_number() over (partition by drvr_user_id order by DLVR_CARRIER_TS) as dlvr_rk
        from base)
    where dlvr_rk <= TOP_N
)

select dlvr_rk,
    count(distinct drvr_user_id) as drvr_cnt,
    avg(online_days) as avg_online_day,
    any_value(p25_days) as p25_online_day,
    any_value(median_days) as median_online_day,
    avg(online_tm) as avg_online_tm,
    any_value(p25_tm) as p25_online_tm,
    any_value(median_tm) as median_online_tm
from (
    select *,
        percentile_cont(online_tm, 0.5) over (partition by dlvr_rk) as median_tm,
        percentile_cont(online_tm, 0.25) over (partition by dlvr_rk) as p25_tm,
        percentile_cont(online_days, 0.5) over (partition by dlvr_rk) as median_days,
        percentile_cont(online_days, 0.25) over (partition by dlvr_rk) as p25_days
    from (
        select dlvr.drvr_user_id,
            dlvr_rk,
            count(distinct b.ACTV_DT) as online_days,
            sum(b.ACTV_DUR_MIN_QTY) as online_tm
        from dlvr
        left join (
            select *
            from `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.DRVR_APPLN_ACTV`
            where drvr_user_id in (select * from drvr_lst)
                and ACTV_DUR_MIN_QTY > 0 -- there're some negative numbers in the table, not sure the cause.
                and ACTV_NM = 'ONLINE'
        ) b 
        on dlvr.drvr_user_id = b.drvr_user_id
        and b.ACTL_ACTV_END_TS between dlvr.rolling_week_ts and dlvr.DLVR_CARRIER_TS
        group by 1,2
    )
)
group by 1
order by 1