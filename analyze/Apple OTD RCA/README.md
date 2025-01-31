# Apple OTD RCA
## Problem Statement
Walmart GoLocal's contract with Apple states OTD of 99.5%.
On average we're performing around 95%. The goal of this analysis is to investigate why Apple OTD is lower than promised and generate actionable insights.

## Conclusion
PENDING

## Approach
1. SLA types
2. Local slot hours
3. Total last mile time break down
4. Driver deliver for same store, would performance get better?
5. Store, distance to walmart stores

## Analysis
### 1. SLA Type
Apple has 2 different SLAs, almost all orders falls into 2 buckets: **60** minutes and **90** minutes.  
This is from slot start time to slot end time, show as **1hr** and **2hrs** in **SLA_Type** field in our data source.  
We see a clear OTD difference between 2 different SLA types.
