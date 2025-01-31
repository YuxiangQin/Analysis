/*
    Extra clean steps - Start 
*/

/*
    UPDATE `wmt-tebi.RogerQin.SparkWPlusTier1Driver`
    SET Year_Reward = '2024'
    where Year_Reward is null; 
*/

/*
    Create a new field, map Month string to number, for better ordering and join operations. 

    CREATE OR REPLACE TABLE `wmt-tebi.RogerQin.SparkWPlusTier1Driver` AS
    SELECT *, 
    CASE 
      WHEN Month_Reward = 'Jan' THEN 1
      WHEN Month_Reward = 'Feb' THEN 2
      WHEN Month_Reward = 'Mar' THEN 3
      WHEN Month_Reward = 'Apr' THEN 4
      WHEN Month_Reward = 'May' THEN 5
      WHEN Month_Reward = 'Jun' THEN 6
      WHEN Month_Reward = 'Jul' THEN 7
      WHEN Month_Reward = 'Aug' THEN 8
      WHEN Month_Reward = 'Sep' THEN 9
      WHEN Month_Reward = 'Oct' THEN 10
      WHEN Month_Reward = 'Nov' THEN 11
      WHEN Month_Reward = 'Dec' THEN 12
    END as Month_Reward_Num
    FROM `wmt-tebi.RogerQin.SparkWPlusTier1Driver`
*/

/*
    Extra clean steps - End 
*/


/* Create a base table for analysis */
DECLARE END_DT DATE DEFAULT DATE('2024-11-30'); -- date used to calculate driver age and tenure

CREATE OR REPLACE TABLE `wmt-tebi.RogerQin.SparkWPlus_RedemptionBase` AS
(
  with base as (
    select distinct Drvr_User_Id, year_reward, month_reward 
    from `wmt-tebi.RogerQin.SparkWPlusTier1Driver`
    cross join unnest([2024]) as year_reward
    cross join unnest([3,4,5,6,8,9,10,11]) as month_reward
  ),

  drvr_state as (
    select Email_id, 
      any_value(coalesce(STATE,
        CASE 
          WHEN UPPER(RIGHT(CBSA, 2)) IN ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY')
          then UPPER(RIGHT(CBSA, 2))
        end)
      ) as state,
      date_diff(END_DT, min(Approved_On), day) as approved_days, 
      date_diff(END_DT, date(min(First_Activation_TS)), day) as activation_days
    from `wmt-driver-insights.sid_dx.TRE_ST_DRVR_COMM_DATA`
    group by 1
  ),

  drvr_age as (
    SELECT DRVR_USER_ID,
      DATE_DIFF(END_DT, DRVR_BIRTH_DT, YEAR) AS age,
      CASE
        WHEN DATE_DIFF(END_DT, DRVR_BIRTH_DT, YEAR) < 25 THEN '< 25'
        WHEN DATE_DIFF(END_DT, DRVR_BIRTH_DT, YEAR) BETWEEN 25 AND 34 THEN '25-34'
        WHEN DATE_DIFF(END_DT, DRVR_BIRTH_DT, YEAR) BETWEEN 35 AND 44 THEN '35-44'
        WHEN DATE_DIFF(END_DT, DRVR_BIRTH_DT, YEAR) BETWEEN 45 AND 54 THEN '45-54'
        WHEN DATE_DIFF(END_DT, DRVR_BIRTH_DT, YEAR) BETWEEN 55 AND 64 THEN '55-64'
        WHEN DATE_DIFF(END_DT, DRVR_BIRTH_DT, YEAR) >= 65 THEN '65+'
        ELSE 'No Birthday Data'
      END AS age_bucket
    FROM (
      select drvr_user_id, 
        DRVR_BIRTH_DT,
        row_number() over (partition by drvr_user_id order by SRC_REC_UPD_TS desc) as row_nbr
      from `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_SECURE.DRVR`
    )
    where row_nbr = 1
  ),

  redem as (
    select
      -- basic info 
      base.drvr_user_id,
      base.year_reward as year_perf,
      base.month_reward - 1 as month_perf,
      base.month_reward,
      -- driver specifics
      a.Past_Month_Completed_Trips,
      a.Customer_Ratings,
      a.Market_Nm,
      drvr_state.state,
      drvr_state.approved_days,
      drvr_state.activation_days,
      drvr_age.age,
      drvr_age.age_bucket,
      -- qualify and redeem
      case when a.Month_Reward_Num is not null then 1 else 0 end as driver_qualified,
      sum(case when b.redem_month is not null then 1 else 0 end) as code_redemed,
      STRING_AGG(DISTINCT CAST(b.redem_month AS STRING), ',') AS redem_months
    from base
    left join (
      select *
      from `wmt-tebi.RogerQin.SparkWPlusTier1Driver`
      where Drvr_Loyalty_Tier = 'Tier 1' --filter out some outlier tier0 drivers
    ) a 
      on base.drvr_user_id = a.Drvr_User_Id
        and base.year_reward = cast(a.Year_Reward as INT64)
        and base.month_reward = a.Month_Reward_Num
    left join (
      select EMAIL_ID,
        redem_year,
        redem_month,
        order_plcd_month_str,
        total_orders_amount,
        string_agg(PROMO_CD, ",") as PROMO_CD_ALL
      from `wmt-tebi.RogerQin.SparkWPlusRedemption`
      group by 1,2,3,4,5) b
      on a.Drvr_User_Id = b.EMAIL_ID
        and (a.Month_Reward_Num = b.redem_month --redeemed same month
          or
          a.Month_Reward_Num = b.redem_month - 1) --redeemed next month 
        and cast(a.Year_Reward as INT64) = b.redem_year
    left join drvr_state on base.drvr_user_id = drvr_state.email_id 
    left join drvr_age on base.drvr_user_id = drvr_age.drvr_user_id
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
    order by 1,2,3,4
  )

  select *,
    case 
        when month_reward >= 8 and most_recent_redem_month_reward <= 6 
            then month_reward - most_recent_redem_month_reward - 1 -- no July data, skip 1 month
        else month_reward - most_recent_redem_month_reward 
    end as redem_month_gap
  from (
    select *,
        -- previous redeemed code count
        sum(code_redemed) over (partition by drvr_user_id order by year_perf, month_reward 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as prev_cum,
        -- most recent redemption month, as reward month
        MAX(
            CASE WHEN code_redemed > 0 THEN month_reward ELSE NULL END
        ) OVER (
            PARTITION BY Drvr_User_Id ORDER BY year_perf, month_reward
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS most_recent_redem_month_reward,
        -- driver qualified next_month
        LEAD(driver_qualified, 1) OVER (PARTITION BY drvr_user_id ORDER BY month_perf) AS qualified_next_month
    from redem
  )
);

/* High level summary */
select year_perf, month_perf, month_reward,
  sum(driver_qualified) as qualified_drvr_cnt,
  sum(case when code_redemed > 0 then 1 else 0 end) as redem_cnt -- to avoid double counting redemption, for example code_redemed > 1. 
FROM `wmt-tebi.RogerQin.SparkWPlus_RedemptionBase`
group by 1,2,3
order by 1,2,3
;

/* Driver who redeemed in the past but didn't redeem that month */
select year_perf,
    month_perf,
    month_reward,
    count(distinct drvr_user_id) as driver_redem_past_not_redem
from `wmt-tebi.RogerQin.SparkWPlus_RedemptionBase`
where prev_cum > 0
  and driver_qualified = 1
  and code_redemed > 0
group by 1,2,3
order by 1,2,3
;

/* Driver who redeemed in the past, and redeemed again */
-- By month
select year_perf,
    month_perf,
    month_reward,
    avg(redem_month_gap) as avg_gap,
    APPROX_QUANTILES(redem_month_gap, 2)[OFFSET(1)] as median_gap,
    APPROX_QUANTILES(redem_month_gap, 4)[OFFSET(3)] AS third_quartile_gap
from `wmt-tebi.RogerQin.SparkWPlus_RedemptionBase`
where code_redemed > 0
    and most_recent_redem_month_reward is not null
group by 1,2,3
order by 1,2,3
;

-- Overall
select 
    avg(redem_month_gap) as avg_gap,
    APPROX_QUANTILES(redem_month_gap, 2)[OFFSET(1)] as median_gap,
    APPROX_QUANTILES(redem_month_gap, 4)[OFFSET(3)] AS third_quartile_gap
from `wmt-tebi.RogerQin.SparkWPlus_RedemptionBase`
where code_redemed > 0
    and most_recent_redem_month_reward is not null
;

/* Drivers that qualify, redeem the code, continue to qualify MoM */
select year_perf,
  month_perf,
  month_reward,
  count(distinct drvr_user_id) as drvr_redem_qualify_mom
from `wmt-tebi.RogerQin.SparkWPlus_RedemptionBase`
where driver_qualified > 0
  and code_redemed > 0
  and qualified_next_month > 0
group by 1,2,3
order by 1,2,3
;

/* Drivers who never redeemed once */

select 
  qualify_cnt,
  count(drvr_user_id) as drvr_cnt
from
(select drvr_user_id,
  sum(driver_qualified) as qualify_cnt,
  sum(code_redemed) as redem_cnt
from `wmt-tebi.RogerQin.SparkWPlus_RedemptionBase`
group by 1)
where redem_cnt = 0
group by 1
order by 1 desc 
;

/* Driver demographic slice */
select year_perf, month_perf, month_reward,
  case when code_redemed > 0 then 1 else 0 end as redeemed,
  avg(age) as avg_age,
  avg(approved_days) as avg_approve_days,
  avg(activation_days) as avg_actv_days,
  avg(Past_Month_Completed_Trips) as avg_past_month_trips,
  avg(Customer_Ratings) as avg_rating
FROM `wmt-tebi.RogerQin.SparkWPlus_RedemptionBase`
group by 1,2,3,4
order by 1,2,3,4
;

select 
  case when redem_cnt = 0 then 1 else 0 end as never_rdeem,
  state,
  count(distinct drvr_user_id) as drvr_cnt,
  avg(age) as avg_age,
  avg(activation_days) as avg_actv_days
from
(select drvr_user_id,
  state,
  approved_days,
  activation_days,
  age,
  age_bucket,
  sum(driver_qualified) as qualify_cnt,
  sum(code_redemed) as redem_cnt
from `wmt-tebi.RogerQin.SparkWPlus_RedemptionBase`
group by 1,2,3,4,5,6)
group by 1,2
order by 1,2
;



