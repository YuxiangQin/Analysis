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
  select drvr_user_id,
    TRIP_ID,
    min(CARRIER_DLVR_TS_TZ) as first_dlvr_ts,
    count(distinct case when On_Time_Delivery = 'Y' then 
      SALES_ORDER_NUM || coalesce(po_num, " ") end) as on_tm_dlvr_cnt,
    count(distinct SALES_ORDER_NUM || coalesce(po_num, " ")) as dlvr_cnt
  from `wmt-driver-insights.LMD_DA.SPARK_DELIVERY_DS_ALL_FINAL`
  where CARRIER_DLVR_TS_TZ is not null -- delivered
    and drvr_user_id in (select * from drvr_lst)
    and date(CARRIER_DLVR_TS_TZ) <= DLVR_END_DT
  group by 1, 2
),

dlvr as (
    select *
    from (select *, row_number() over (partition by drvr_user_id order by first_dlvr_ts) as dlvr_rk
        from base)
    where dlvr_rk <= TOP_N
)

select dlvr_rk as trip_rk,
    count(distinct drvr_user_id) as drvr_cnt,
    sum(dlvr_cnt) as tot_dlvr_cnt,
    sum(on_tm_dlvr_cnt) as on_tm_dlvr_cnt, 
    sum(on_tm_dlvr_cnt) / sum(dlvr_cnt) as OTD
from dlvr 
group by 1
order by 1