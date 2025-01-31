-- Online Time Benchmark
with drvr_lst as (
  select distinct DRVR_USER_ID
  from `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.DRVR`
  where DRVR_SVC_TYPE_NM = 'REGULAR' -- excluding drivers from homeoffice and test
)

select WM_WEEK_NBR,
  count(distinct drvr_user_id) as drvr_cnt,
  any_value(p25_tm) as p25_tm,
  any_value(p30_tm) as p30_tm,
  any_value(p35_tm) as p35_tm,
  any_value(p40_tm) as p40_tm,
  any_value(p45_tm) as p45_tm,
  any_value(median_tm) as median_tm,
  any_value(p25_days) as p25_days,
  any_value(p30_days) as p30_days,
  any_value(p35_days) as p35_days,
  any_value(p40_days) as p40_days,
  any_value(p45_days) as p45_days,
  any_value(median_days) as median_days
from (
  select *,
    percentile_cont(online_tm_wk, 0.5) over (partition by WM_WEEK_NBR) as median_tm,
    percentile_cont(online_tm_wk, 0.25) over (partition by WM_WEEK_NBR) as p25_tm,
    percentile_cont(online_tm_wk, 0.30) over (partition by WM_WEEK_NBR) as p30_tm,
    percentile_cont(online_tm_wk, 0.35) over (partition by WM_WEEK_NBR) as p35_tm,
    percentile_cont(online_tm_wk, 0.40) over (partition by WM_WEEK_NBR) as p40_tm,
    percentile_cont(online_tm_wk, 0.45) over (partition by WM_WEEK_NBR) as p45_tm,
    percentile_cont(online_days, 0.5) over (partition by WM_WEEK_NBR) as median_days,
    percentile_cont(online_days, 0.25) over (partition by WM_WEEK_NBR) as p25_days,
    percentile_cont(online_days, 0.30) over (partition by WM_WEEK_NBR) as p30_days,
    percentile_cont(online_days, 0.35) over (partition by WM_WEEK_NBR) as p35_days,
    percentile_cont(online_days, 0.40) over (partition by WM_WEEK_NBR) as p40_days,
    percentile_cont(online_days, 0.45) over (partition by WM_WEEK_NBR) as p45_days
  from (
    select a.drvr_user_id,
      CDT.WM_YEAR_WK_NBR as WM_WEEK_NBR,
      count(distinct ACTV_DT) as online_days,
      sum(ACTV_DUR_MIN_QTY) online_tm_wk
    from `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.DRVR_APPLN_ACTV` a 
    left join `wmt-edw-prod.US_CORE_DIM_VM.CALENDAR_DIM` CDT
      on date(a.ACTV_DT) = CDT.CALENDAR_DATE
    inner join (
      select distinct DRVR_USER_ID, WM_WK
      from `wmt-driver-insights.LMD_DA.SPARK_DELIVERY_DS_ALL_FINAL`
      where left(WM_WK, 4) = '2023'
    ) d -- driver at least asscociated with one trip
    on CDT.WM_YEAR_WK_NBR = safe_cast(d.WM_WK as int64)
      and a.drvr_user_id = d.drvr_user_id
    where CDT.WM_YEAR_NBR = 2023
      and a.drvr_user_id in (select * from drvr_lst)
      and ACTV_DUR_MIN_QTY > 0 -- there're some negative numbers in the table, not sure the cause.
      and ACTV_NM = 'ONLINE'
    group by 1,2
  )
)
group by 1
order by 1