DECLARE START_DT DATE DEFAULT current_date("America/Los_Angeles") - 180;
DECLARE END_DT DATE DEFAULT current_date("America/Los_Angeles") - 1;
DECLARE LARGE_PAY_AMT INT64 DEFAULT 30; -- large payment threshold, need confirmation from stakeholder, now using number close to 95 percentile
DECLARE MAX_EXPRESS_OFFR_ACCPT_TIME INT64 DEFAULT 3; -- express offer accepted threshold, in seconds

CREATE OR REPLACE TABLE `wmt-tebi.RogerQin.driver_fraud_anomaly_detection_base_table` as (

WITH 

/* Missing Items */
missing_items AS (
  SELECT 
    b.DRVR_USER_ID,
    SUM(ITEM_QTY) AS ITEM_QTY,
    SUM(IF(a.MISSING_PO_IND = 1 AND a.Type_of_Missing_Order IN  ("Full Missing Order", "Partial Missing Order"), a.RTN_QTY, 0)) AS MISSING_ITEM_QTY, 
    SUM(basket) as ordered_dollar_value,
    SUM(IF(a.MISSING_PO_IND = 1 AND a.Type_of_Missing_Order IN  ("Full Missing Order", "Partial Missing Order"), a.total_refund_amt, 0)) AS MISSING_ITEM_DOLLAR_VALUE
  FROM `wmt-tebi.jerome.LMD_DISPATCHED_PO_WITH_MISSING` a 
  INNER JOIN (
    SELECT distinct WM_YEAR_WK_NBR
    FROM `wmt-edw-prod.US_CORE_DIM_VM.CALENDAR_DIM`
    WHERE CALENDAR_DATE between START_DT and END_DT
  ) cal
  ON a.WM_WK = cal.WM_YEAR_WK_NBR
  LEFT JOIN (
    SELECT distinct SALES_ORDER_NBR,
      PO_NBR,
      DRVR_USER_ID
    FROM `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_SECURE.DLVR_LAST_MI_DTL`
    WHERE ORDER_PLCD_DT between START_DT and END_DT
      AND DLVR_CARRIER_NM = 'SPARK'
      AND DLVR_CARRIER_TS IS NOT NULL
  ) b 
    ON a.SALES_ORDER_NUM = b.SALES_ORDER_NBR
      AND a.PO_NUM = b.PO_NBR 
  WHERE a.carrier = 'SPARK' 
  GROUP BY 1
),

/* Missing POs */
missing_pos AS (
  select drvr_user_id,
    count(distinct PO_NUM) as missing_po_cnt
  from `wmt-lmd-data-science-prod.LMD_DRVR_ABUSE.MISSING_PO_2_0_ABUSE_CATEGORY_SINCE_OCT23` 
  where RPT_DT between START_DT and END_DT
    and ABUSE_CATEGORY != 'CUSTOMER_FRAUD' -- filter out non-driver related
  group by 1
),

/* Driver phone number and email */
drvr_info as (
  select *
  from
  (
    select DRVR_USER_ID,
        DRVR_PH_NBR,
        trim(lower(DRVR_EMAIL_ID)) as drvr_email_id,
        DRVR_ONBDG_TS,
        DRVR_RATG_NBR,
        DEACTIVATION_TYPE_NM,
        DEACTIVATION_RSN_NM,
        LAST_ACTIVATED_TS,
        LAST_DEATVD_TS,
        row_number() over (partition by DRVR_USER_ID order by SNAPSHOT_EFF_END_DT desc) rk
    from `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_SECURE.DRVR`
  )
  where rk = 1 -- latest info
),

cust_info as (
  select distinct CUST_ID,
    trim(lower(email_id)) as cust_email_id,
    ph_nbr as cust_ph_nbr
  from `wmt-edw-prod.WW_CUSTOMER_DL_SECURE.CUST_PROFL`
),

same_ph_email as (
  select dlmd.DRVR_USER_ID,
    count(distinct case when cust_info.cust_email_id = drvr_info.drvr_email_id then trip_id end) as same_cust_email_trip_cnt,
    count(distinct case when cust_info.cust_ph_nbr = drvr_info.drvr_ph_nbr then trip_id end) as same_cust_phone_trip_cnt,
    count(distinct case when cust_info.cust_ph_nbr = drvr_info.drvr_ph_nbr 
      or cust_info.cust_email_id = drvr_info.drvr_email_id then trip_id end) as same_cust_email_phone_trip_cnt
  from `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_SECURE.DLVR_LAST_MI_DTL` dlmd
  left join cust_info
    on dlmd.BILL_TO_CUST_ID = cust_info.CUST_ID
  left join drvr_info on dlmd.DRVR_USER_ID = drvr_info.DRVR_USER_ID
  where ORDER_PLCD_DT between START_DT and END_DT
    and (
      cust_info.cust_email_id = drvr_info.drvr_email_id
      or
      cust_info.cust_ph_nbr = drvr_info.drvr_ph_nbr
    )
group by 1),

/* Driver Payments */
large_pay_cnt AS (
    SELECT DRVR_USER_ID,
        COUNT(*) AS large_pay_amt_cnt
    FROM `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.LAST_MI_DLVR_DRVR_PYMT`
    WHERE PYMT_RQ_DT BETWEEN START_DT AND END_DT
        AND PYMT_TRANS_TYPE_CD = 'ORDER_PAYMENT' -- only filter order payment to avoid noises
        AND carrier_nm = 'SPARK'
        AND PYMT_TRANS_AMT > LARGE_PAY_AMT
    GROUP BY 1
),
freq_table AS (
  select *
  from (
    SELECT *,
        RANK() OVER (PARTITION BY DRVR_USER_ID ORDER BY frequency desc, PYMT_TRANS_AMT) as freq_rk -- make sure only one record per driver, assume repeated low amount is more likely fraud behavior
    FROM (
        SELECT 
            DRVR_USER_ID, 
            PYMT_TRANS_AMT, 
            COUNT(*) AS frequency
        FROM `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.LAST_MI_DLVR_DRVR_PYMT`
        WHERE PYMT_RQ_DT between START_DT and END_DT
          and PYMT_TRANS_TYPE_CD = 'ORDER_PAYMENT' -- only filter order payment to avoid noises
          and carrier_nm = 'SPARK'
        GROUP BY 1,2
    )
  )
  where freq_rk = 1
),

driver_payments AS (
  SELECT large_pay_cnt.DRVR_USER_ID,
      large_pay_cnt.large_pay_amt_cnt,
      freq_table.PYMT_TRANS_AMT as max_freq_amt,
      freq_table.frequency as max_freq_amt_cnt
  FROM large_pay_cnt
  JOIN freq_table ON large_pay_cnt.DRVR_USER_ID = freq_table.DRVR_USER_ID
),

/* Driver Dropped/Cancelled after arrived at store */
drvr_drop as (
  select distinct drvr_user_id,
        trip_id,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',src_cre_ts) as dropped_time 
  from `wmt-tebi.Prashanth_AE_FY24.Driver_DroppedTrips`
  where date(src_cre_ts) between START_DT and END_DT
    and CARRIER_STATUS_CD_DESC in ('DRIVER_DROPPED', 'DRIVER_CANCELLED')
    and trip_id is not null 
    and drvr_user_id is not null
),

arrived_at_store as (
select distinct po_num, 
      SRC_SALES_ORDER_NUM,
      trip_id,
      max(drvr_user_id) over(partition by po_num,trip_id,FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',src_cre_ts)) as drvr_user_id,
      FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',src_cre_ts) as arrived_ts,
      FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',src_cre_ts_utc) as arrived_ts_utc,
from `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.LAST_MI_DLVR_TASK_CHNG_EVENT` LMDTS
WHERE SRC_CRE_DT between START_DT and END_DT
  and DATA_SRC_CD = 'DISPATCHER' 
  AND CARRIER_NM='SPARK' 
  and CARRIER_STATUS_CD_DESC='ARRIVED_AT_STORE' 
  and po_num is not null
  and drvr_user_id is not null
),

drop_after_arrive_at_store as (
  select b.drvr_user_id,
    count(distinct b.trip_id) as drop_after_arrived_at_store_trip_cnt
  from arrived_at_store a  
  left join drvr_drop b 
    on a.drvr_user_id = b.drvr_user_id 
      and a.trip_id = b.trip_id
  where b.dropped_time > a.arrived_ts
  group by 1
),

/* Express offer accept */
express_offer_accept AS (
    select drvr_user_id,
        count(distinct trip_id) as total_accpt_trip_cnt,
        count(distinct case when time_diff_seconds < MAX_EXPRESS_OFFR_ACCPT_TIME then trip_id end) as express_accpt_trip_cnt
    from (
        select 
            DLVR_OFFR_TRIP_ID AS TRIP_ID,
            drvr_user_id,
            min(DLVR_OFFR_CRE_TS_UTC) as OFFER_CREATE_TS_UTC,
            min(DLVR_OFFR_MODFD_TS_UTC) as OFFER_ACCEPT_TS_UTC,
            TIMESTAMP_DIFF(min(DLVR_OFFR_MODFD_TS_UTC), min(DLVR_OFFR_CRE_TS_UTC), SECOND) as time_diff_seconds
        from `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.CARRIER_DLVR_OFFR`
        where DLVR_OFFR_CRE_DT between START_DT and END_DT
            and OFFR_RND_NBR is not null -- Only Spark Offers
            and lower(DLVR_OFFR_STS_NM) like '%accepted%'
        group by 1,2
    )
    group by 1
),

/* Existing Model */
existing_model AS (
  select drvr_user_id,
    count(distinct case when swipe_abuse =1 then trip_id end) as swipe_abuse_trip_cnt,
    count(distinct case when RETURN_ABUSE =1 then trip_id end) as return_abuse_trip_cnt,
    count(distinct case when SCAN_ABUSE =1 then trip_id end) as scan_abuse_trip_cnt,
    count(distinct case when DROP_ABUSE =1 then trip_id end) as drop_abuse_trip_cnt,
    count(distinct case when MULTI_DROP_ABUSE =1 then trip_id end) as multi_drop_abuse_trip_cnt,
    count(distinct case when PROP_22_ABUSE =1 then trip_id end) as prop_22_abuse_trip_cnt,
    count(distinct case when SHOP_N_DELIVER_NIL_PICK_ABUSE =1 then trip_id end) as shop_n_deliver_nil_pick_abuse_trip_cnt
  from `wmt-lmd-data-science-prod.LMD_DRVR_ABUSE.tk_ALL_ABUSE_FLAGGED_SINCE_JULY2021`
  where OFFR_DT between START_DT and END_DT
  group by 1
),

/* Basic delivery data */
deliveries as (
    select drvr_user_id,
        count(distinct SRC_SALES_ORDER_NUM) as total_order_cnt,
        count(distinct coalesce(PO_NUM, DLVR_REF_ID)) as total_po_cnt,
        count(distinct trip_id) as total_trip_cnt
    from `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_SECURE.LAST_MI_DLVR_TASK_CHNG_EVENT`
    where SRC_CRE_DT between START_DT and END_DT
        and CARRIER_NM = 'SPARK'
    group by 1
)

-- Final SELECT statement to join all the CTEs (Common Table Expressions)
SELECT 
    eo.DRVR_USER_ID,
    TIMESTAMP_DIFF(END_DT, dr.DRVR_ONBDG_TS, DAY) AS driver_tenure_day,
    dr.DRVR_RATG_NBR,
    dr.DEACTIVATION_TYPE_NM,
    dr.DEACTIVATION_RSN_NM,
    dr.LAST_ACTIVATED_TS,
    dr.LAST_DEATVD_TS,
    COALESCE(dlvr.total_order_cnt, 0) AS total_order_cnt,
    COALESCE(dlvr.total_po_cnt, 0) AS total_po_cnt,
    COALESCE(dlvr.total_trip_cnt, 0) AS total_trip_cnt,
    COALESCE(eo.total_accpt_trip_cnt, 0) AS total_accpt_trip_cnt,
    COALESCE(mi.ITEM_QTY, 0) AS total_item_cnt,
    COALESCE(mi.MISSING_ITEM_QTY, 0) AS missing_item_cnt,
    COALESCE(mi.ordered_dollar_value, 0) AS ordered_dollar_value,
    COALESCE(mi.MISSING_ITEM_DOLLAR_VALUE, 0) AS MISSING_ITEM_DOLLAR_VALUE,
    COALESCE(mp.missing_po_cnt, 0) AS missing_po_cnt,
    COALESCE(di.same_cust_email_trip_cnt, 0) AS same_cust_email_trip_cnt,
    COALESCE(di.same_cust_phone_trip_cnt, 0) AS same_cust_phone_trip_cnt,
    COALESCE(di.same_cust_email_phone_trip_cnt, 0) AS same_cust_email_phone_trip_cnt,
    COALESCE(dp.large_pay_amt_cnt, 0) AS large_pay_amt_cnt,
    COALESCE(dp.max_freq_amt, 0) AS max_freq_amt,
    COALESCE(dp.max_freq_amt_cnt, 0) AS max_freq_amt_cnt,
    COALESCE(dd.drop_after_arrived_at_store_trip_cnt, 0) AS drop_after_arrived_at_store_trip_cnt,
    COALESCE(eo.express_accpt_trip_cnt, 0) AS express_accpt_trip_cnt,
    COALESCE(em.swipe_abuse_trip_cnt, 0) AS swipe_abuse_trip_cnt,
    COALESCE(em.return_abuse_trip_cnt, 0) AS return_abuse_trip_cnt,
    COALESCE(em.scan_abuse_trip_cnt, 0) AS scan_abuse_trip_cnt,
    COALESCE(em.drop_abuse_trip_cnt, 0) AS drop_abuse_trip_cnt,
    COALESCE(em.multi_drop_abuse_trip_cnt, 0) AS multi_drop_abuse_trip_cnt,
    COALESCE(em.prop_22_abuse_trip_cnt, 0) AS prop_22_abuse_trip_cnt,
    COALESCE(em.shop_n_deliver_nil_pick_abuse_trip_cnt, 0) AS shop_n_deliver_nil_pick_abuse_trip_cnt,
    START_DT as data_start_dt_pt,
    END_DT as data_end_dt_pt
FROM 
    express_offer_accept eo
    left join drvr_info dr on eo.drvr_user_id = dr.drvr_user_id
    left join deliveries dlvr on eo.drvr_user_id = dlvr.drvr_user_id
    LEFT JOIN missing_items mi ON eo.DRVR_USER_ID = mi.DRVR_USER_ID
    LEFT JOIN missing_pos mp ON eo.DRVR_USER_ID = mp.DRVR_USER_ID
    LEFT JOIN same_ph_email di ON eo.DRVR_USER_ID = di.DRVR_USER_ID
    LEFT JOIN driver_payments dp ON eo.DRVR_USER_ID = dp.DRVR_USER_ID
    LEFT JOIN drop_after_arrive_at_store dd ON eo.DRVR_USER_ID = dd.DRVR_USER_ID
    LEFT JOIN existing_model em ON eo.DRVR_USER_ID = em.DRVR_USER_ID
)