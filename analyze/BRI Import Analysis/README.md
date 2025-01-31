# BRI Import Analysis

## Data Preparation
Data are gathered from Spine item level report [here](https://spine.prod.walmart.com/reportCenter#itemLevelReport).

*Use Microsoft Edge broswer to open Spine link, if it's not working in Chrome.
*Note the limit of each report download is 1 million rows.  
  
**Time Frame**  
Timeframe: Last 52 Weeks; Time Selection: Time Across Data Down By WM Month  
  
**Local Filters**   
US - US Stores (Some of the metrics required not present when chosse Online)  
Hierarchy: SBU (Select the SBU you want to download)  
  
**Report Columns**  
- SBU
- Department Desc
- Category Desc
- Channel
- Country of Origin
- Private Brand
- TY POS Sales
- TY POS Qty
- LY POS Sales
- LY POS Qty
- Average Unit Cost

## File Description
`data process.ipynb`: contains scripts to process data, input files store in a local folder.
