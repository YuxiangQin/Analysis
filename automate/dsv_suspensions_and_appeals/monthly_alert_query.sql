DECLARE RPT_DT DATE DEFAULT DATE_SUB(current_date('America/Los_Angeles'), INTERVAL 2 MONTH); -- always look at 2 months back

select distinct year,
  month,
  format_date('%B-%Y', RPT_DT) as month_year,
  VENDOR_ID,
  VENDOR_NAME,
  if(Late_T = 'Over', 'Late Chargebacks', NULL) as Late_T,
  if(Reject_T = 'Over', 'Reject Chargebacks', NULL) as Reject_T,
  if(TCB_T = 'Over', 'Transportation Chargebacks', NULL) as TCB_T,
  b.VendorEmails,
  b.OwnerEmail,
  b.owner,
  b.multi_owner_flag,
  b.owner_match_flag,
  case
    when b.multi_owner_flag = TRUE then 'ACTION NEEDED - Multiple values for Owner in Contacts sheet'
    when b.OWNER_MATCH_FLAG = FALSE then 'ACTION NEEDED - Owner in Contacts Sheet differs from R2D2 mapping'
    when b.vendoremails is null then 'ACTION NEEDED - No email contact information for this vendor'
    else NULL
  END as Comments
from `wmt-outbound-bi.DSV.DSV_CB_Dash_SNA` a
left join `wmt-tebi.DSV.VendorEmailList` b on a.VENDOR_ID = b.VendorID
where year = extract(year from RPT_DT)
and month = extract(month from RPT_DT)
and (
  Late_T = 'Over'
  or REJECT_T = 'Over'
  or TCB_T = 'Over'
)
and netunits > 1000 -- volume threshold
