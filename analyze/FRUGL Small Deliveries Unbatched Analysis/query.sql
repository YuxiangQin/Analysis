-- Focus only on small deliveries, and few DLVR_STS that might be batched
select *,
    EXTRACT(HOUR FROM SLOT_START_TS_UTC AT TIME ZONE (CLNT_PCKUP_FROM_TZ_CD)) as local_slot_hr_start,
    EXTRACT(HOUR FROM SLOT_END_TS_UTC AT TIME ZONE (CLNT_PCKUP_FROM_TZ_CD)) as local_slot_hr_end,
    DATE(SLOT_START_TS_UTC, CLNT_PCKUP_FROM_TZ_CD) as local_slot_dt
from `wmt-edw-sandbox.LM_IAD_DAAS.DPS_DLVR_LAST_MI_DTL`
where SLA_Type = 'FRUGL'
    and DLVR_PKG_SIZE_CD = 'S'
    and upper(DLVR_STS) in ('DELIVERED',
                            'RETURNED',
                            'INCOMPLETE DELIVERED',
                            'INCOMPLETE RETURNED')
