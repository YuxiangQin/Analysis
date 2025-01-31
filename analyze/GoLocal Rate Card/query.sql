/*******************************************************
rate card query for 1-800 flowers and Sally Beauty
********************************************************/

-- build a base table for invoice calculation
create or replace table `wmt-edw-sandbox.LM_IAD_DAAS.GOLOCAL_RATE_CARD_BASE` as (
  select A.*,
    coalesce(
      CLNT_TRIP_STATUS_NM,
      DLVR_STS,
      case when DLVR_CARRIER_TS_UTC is not null then 'DELIVERED'
        when coalesce(RTN_TS_UTC, RTN_TO_STORE_TS_UTC) is not null then 'RETURNED'
        end
      ) as rate_status,
    coalesce(
      A.DISTN_EST_MI_QTY,
      INV.DISTN_SHORTEST_RD_MI_QTY,
      TOT_DISTN_DRVN_EST_MI_QTY
      ) as rate_miles,
    case when tenant_id = 1091
      and date_diff(slot_end_ts_utc,slot_start_ts_utc,minute) <= 120
      then 1 else 0 end as express_ind, -- express flag for 1-800 flowers
    case when CNCL_RQ_SRC_NM = 'CLIENT' and
      A.ENRTE_TO_PCKUP_TS_UTC is not null then 1 else 0 end as client_cancel_ind
  from `wmt-edw-sandbox.LM_IAD_DAAS.TMP_LAST_MI_DLVR_DLY_RCA_LEVEL1` A
  LEFT JOIN `wmt-edw-prod.US_FIN_ECOMM_DL_VM.FIN_DLVR_AS_A_SVC_INV_CHRG` INV -- join this table to fill null values in mileage
    ON A.SALES_ORDER_NBR = INV.SRC_SALES_ORDER_NUM
    AND A.DLVR_TASK_ID = INV.DLVR_ID
    AND A.TRIP_ID = INV.TRIP_ID
    -- pulling tip payments from TMP_DLMD therefore only taking delivery payment charges
    AND upper(INV.INV_CHRG_TYPE_NM)='DELIVERY'
  where RPT_DT between '2023-05-08' and current_date -- good to cover these 2 clients, subject to change if adding other clients
    and tenant_id in (
      1030, -- sally beauty
      1091 -- 1-800 flowers
    )
);

-- table stores invoice information
create or replace table `wmt-edw-sandbox.LM_IAD_DAAS.GOLOCAL_INV_CHRG_TEMP` as (
  with dlvr as (
    select distinct tenant_id,
      tenant_nm,
      RPT_DT,
      SALES_ORDER_NBR,
      DLVR_TASK_ID,
      TRIP_ID,
      STORE_NM,
      EXT_STORE_NBR,
      CITY_NM,
      DLVR_AREA_NM,
      BATCH_IND,
      rate_status,
      DLVR_STS,
      rate_miles,
      express_ind,
      client_cancel_ind
    from `wmt-edw-sandbox.LM_IAD_DAAS.GOLOCAL_RATE_CARD_BASE`
  )

  select distinct dlvr.*,
    base.rate as base_rate,
    case when rate_status = 'DELIVERED' and express_ind = 1 then base.rate + 2.5
      when rate_status = 'DELIVERED' then base.rate
      else 0.0 end as BASE_DLVR_CHRG_AMT,
    dist_ovr.overage_rate as dist_overage_rate,
    case when rate_status = 'DELIVERED' and dist_ovr.overage_rate is not null
      then (ceiling(rate_miles) - dist_ovr.min_val) * dist_ovr.overage_rate
      else 0.0 end as DISTN_OVER_CHRG_AMT,
    case when dlvr.tenant_id = 1408 and DWT > 10 then
      (ceiling(DWT) - 10) * 0.5
      else 0.0 end as WAIT_TM_OVER_CHRG_AMT,
    0.0 as TIP_DISC_AMT,
    case when dlvr.tenant_id = 1408 and batch_ind = 1 then 4
      else 0.0 end as BATCH_DISC_AMT,
    0.0 as VOL_DISC_AMT,
    coalesce(
      case when rate_status = 'RETURNED' and dlvr.tenant_id = 1091 then 4.0
        when rate_status = 'RETURNED' and dlvr.tenant_id = 1030
        then base.rate end,
      0.0) as RTN_CHRG_AMT,
    case when rate_status = 'CANCELLED' and client_cancel_ind = 1
      then (case when express_ind = 1 then base.rate + 2.5 else base.rate end)
      + coalesce((ceiling(rate_miles) - dist_ovr.min_val) * dist_ovr.overage_rate, 0.0)
      else 0.0 end as CNCL_CHRG_AMT,
    0.0 as ST_SURFEE_CHRG_AMT,
    0.0 as REIMBMENT_AMT,
    0.0 as DMG_ORDER_DISC_AMT
  from dlvr
  left join `wmt-edw-sandbox.LM_IAD_DAAS.GOLOCAL_BASE_RATE_TEMP` base
    on dlvr.TENANT_ID = base.tenant_id
    and (dlvr.rate_miles > base.min_val or (dlvr.rate_miles = 0 and base.min_val = 0)) -- handle rate_mile == 0 case specially
    and (dlvr.rate_miles <= base.max_val or base.max_val is null) -- valid mileage range
    and dlvr.RPT_DT between base.rate_start_date and base.rate_end_date -- valid rate dates
  left join `wmt-edw-sandbox.LM_IAD_DAAS.GOLOCAL_DISTN_OVER_RATE_TEMP` dist_ovr
    on dlvr.TENANT_ID = dist_ovr.tenant_id
    and dlvr.rate_miles > dist_ovr.min_val -- valid mileage range
    and dlvr.RPT_DT between dist_ovr.rate_start_date and dist_ovr.rate_end_date -- valid rate dates
  where (base.rate is not null)
    or (dist_ovr.overage_rate is not null) -- keep valid records only
);

-- update TMP_LAST_MI_DLVR_DLY_RCA_LEVEL1 table
create or replace table `wmt-edw-sandbox.LM_IAD_DAAS.TMP_LAST_MI_DLVR_DLY_RCA_LEVEL1` as (
  select A.*
    EXCEPT(
      CLNT_BASE_DLVR_CHRG_AMT,
      CLNT_DISTN_OVER_CHRG_AMT,
      CLNT_RTN_CHRG_AMT,
      CLNT_CNCL_CHRG_AMT
      ),
    coalesce(INV_TMP.BASE_DLVR_CHRG_AMT, A.CLNT_BASE_DLVR_CHRG_AMT)
      as CLNT_BASE_DLVR_CHRG_AMT,
    coalesce(INV_TMP.DISTN_OVER_CHRG_AMT, A.CLNT_DISTN_OVER_CHRG_AMT)
      as CLNT_DISTN_OVER_CHRG_AMT,
    coalesce(INV_TMP.RTN_CHRG_AMT, A.CLNT_RTN_CHRG_AMT) as CLNT_RTN_CHRG_AMT,
    coalesce(INV_TMP.CNCL_CHRG_AMT, A.CLNT_CNCL_CHRG_AMT) as CLNT_CNCL_CHRG_AMT
  from `wmt-edw-sandbox.LM_IAD_DAAS.TMP_LAST_MI_DLVR_DLY_RCA_LEVEL1` A
  LEFT JOIN `wmt-edw-sandbox.LM_IAD_DAAS.GOLOCAL_INV_CHRG_TEMP` INV_TMP
    ON A.SALES_ORDER_NBR = INV_TMP.SALES_ORDER_NBR
    AND A.DLVR_TASK_ID = INV_TMP.DLVR_TASK_ID
    AND A.TRIP_ID = INV_TMP.TRIP_ID
)
