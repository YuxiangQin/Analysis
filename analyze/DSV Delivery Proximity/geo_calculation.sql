create or replace table `wmt-tebi.RogerQin.DSV_Proximity_Base_Table_with_GEO_Dist` as (
    select *,
        round(geo_estimated_miles) as rounded_geo_estimated_miles
    from (
        select b.*,
            l.ship_node_lat,
            l.ship_node_lon,
            case 
                when b.ship_node_addr is null 
                    or l.ship_node_lat is null 
                    or b.ship_to_lat is null then b.zip_estimated_miles
                else ST_DISTANCE(
                    ST_GEOGPOINT(cast(l.ship_node_lon as float64), cast(l.ship_node_lat as float64)),
                    ST_GEOGPOINT(b.ship_to_long, b.ship_to_lat)
                ) / 1609.34 end as geo_estimated_miles
        from `wmt-tebi.RogerQin.DSV_Proximity_Base_Table` b
        left join `wmt-tebi.RogerQin.DSV_DCLocationInfo` l 
            on b.DC_ID = l.DC_ID
    )
)

select DC_ID,
    DIST_NAME,
    Division,
    OWNER,
    VENDOR_ID,
    VENDOR_NAME,
    ship_node_addr,
    ship_node_city,
    ship_node_state,
    ship_node_zip,
    sum(case when proximity_bucket = '0-10 miles' then shipped_units else 0 end) as  `0-10 miles delivered units`,
    sum(case when proximity_bucket = '11-20 miles' then shipped_units else 0 end) as  `11-20 miles delivered units`,
    sum(case when proximity_bucket = '21-30 miles' then shipped_units else 0 end) as  `21-30 miles delivered units`,
    sum(case when proximity_bucket = '31-40 miles' then shipped_units else 0 end) as  `31-40 miles delivered units`,
    sum(case when proximity_bucket = '41-50 miles' then shipped_units else 0 end) as  `41-50 miles delivered units`,
    sum(case when proximity_bucket = '51-60 miles' then shipped_units else 0 end) as  `51-60 miles delivered units`
from `wmt-tebi.RogerQin.DSV_Proximity_Base_Table_with_GEO_Dist` b
left join `wmt-tebi.RogerQin.DSV_Proximity_Buckets` p 
    on b.rounded_geo_estimated_miles between p.min_miles and p.max_miles 
where p.proximity_bucket != 'over 60 miles'
group by 1,2,3,4,5,6,7,8,9,10
order by 11 desc







