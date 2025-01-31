DECLARE START_DT DEFAULT DATE '2024-03-01';
DECLARE END_DT DEFAULT DATE '2024-03-31';

-- create a table with proximity buckets to use for join
create or replace table `wmt-tebi.RogerQin.DSV_Proximity_Buckets` as (
    SELECT 
    0 AS min_miles, 10 AS max_miles, '0-10 miles' AS proximity_bucket
    UNION ALL
    SELECT 
    11 AS min_miles, 20 AS max_miles, '11-20 miles' AS proximity_bucket
    UNION ALL
    SELECT 
    21 AS min_miles, 30 AS max_miles, '21-30 miles' AS proximity_bucket
    UNION ALL
    SELECT 
    31 AS min_miles, 40 AS max_miles, '31-40 miles' AS proximity_bucket
    UNION ALL
    SELECT 
    41 AS min_miles, 50 AS max_miles, '41-50 miles' AS proximity_bucket
    UNION ALL
    SELECT 
    51 AS min_miles, 60 AS max_miles, '51-60 miles' AS proximity_bucket
    UNION ALL
    SELECT 
    61 AS min_miles, 99999 AS max_miles, 'over 60 miles' AS proximity_bucket
);

create or replace table `wmt-tebi.RogerQin.DSV_Proximity_Base_Table` as (
    -- base table with address details
    with base as (
        select f360.*,
            r2d2.Division,
            r2d2.OWNER,
            r2d2.DIST_NAME,
            r2d2.VENDOR_ID,
            r2d2.VENDOR_NAME,
            r2d2.ADDRESSLINE1 as ship_node_addr,
            r2d2.CITY as ship_node_city,
            r2d2.STATE as ship_node_state,
            r2d2.ZIP as ship_node_zip,
            addr.ADDR_LINE_1_TXT as ship_to_addr,
            addr.CITY_NM as ship_to_city,
            addr.ST_PROV_NM as ship_to_state,
            addr.ZIP_CD as ship_to_zip,
            addr.LAT_VAL as ship_to_lat,
            addr.LONG_VAL as ship_to_long
        from (
            select PO_NUM,
                cast(SHIP_NODE as int64) as DC_ID,
                SRC_SHIP_TO_ADDR_CD,
                sum(SHPD_QTY) as shipped_units
            from `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.FULFMT_360` 
            where OP_CMPNY_CD = 'WMT.COM'
                and ORDER_PLCD_DT between START_DT and END_DT
                and SHIP_NODE_ORG_TYPE_CD = 'DSV'
                and STATUS_DESC = 'PO_DELIVERED'
            group by 1,2,3
        ) f360
        inner join `wmt-outbound-bi.ADHOC_DATA.DSV_R2D2` r2d2
            on f360.DC_ID = r2d2.Distributor_ID
        left join `wmt-edw-prod.WW_CUSTOMER_DL_SECURE.CUST_ADDR_CNTCT` addr 
            on f360.SRC_SHIP_TO_ADDR_CD = addr.ADDR_ID
        where r2d2.OWNER != 'Unmanaged' -- only managed vendors
    )
    -- calculate distance using zip codes
    select *, round(zip_estimated_miles) as rounded_zip_estimated_miles
    from (select base.*,
            (ST_DISTANCE(ST_GEOGFROMTEXT(OZIP.zipcode_geom), 
                ST_GEOGFROMTEXT(DZIP.zipcode_geom)) * 1.2) / 1609.34 as zip_estimated_miles 
        from base 
        LEFT JOIN (
        select DISTINCT zipcode_geom, zipcode from `bigquery-public-data.utility_us.zipcode_area`
        ) OZIP ON base.SHIP_NODE_ZIP = OZIP.zipcode
        LEFT JOIN (
        select DISTINCT zipcode_geom, zipcode from `bigquery-public-data.utility_us.zipcode_area`
        ) DZIP ON base.SHIP_TO_ZIP = DZIP.zipcode)
)