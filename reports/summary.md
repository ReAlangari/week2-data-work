# Week 2 Summary

## Key Findings
- **Total Revenue:** $145.50 (from 5 total orders)
- **Refund Rate:** 20% (1 refund out of 5 orders)
- **Top Country by Revenue:** Saudi Arabia (SA) - $145.50 (100% of revenue)
- **Monthly Trend:** Revenue appears artificially low in December due to one $100 order missing month assignment

## Definitions
- **Revenue:** Sum of `amount` for all orders where `status_clean` ≠ "refund"
- **Refund Rate:** `count(status_clean == 'refund') / total_orders`
- **Time Window:** All available data in dataset (current range: Dec 1-2, 2025)

## Data Quality Caveats
- **Sample Size:** Only 5 total orders - too small for statistical significance
- **Missing Data:** One $100 order missing month assignment, skewing monthly trend charts
- **Country Imbalance:** Saudi Arabia (SA) has 4 orders, UAE (AE) has only 1 order
- **Temporal Coverage:** No data for "Day 4" (Dec 4, 2025) yet
- **Outliers:** Amount winsorized at 99th percentile for visualization

## Critical Limitations
1. **Cannot perform statistical tests** with n=5
2. **Bootstrap results are invalid** - AE has only 1 order (100% refund rate)
3. **Monthly trend is distorted** by missing $100 order month
4. **"Day 4 analysis" not possible** - data only goes to Dec 2

## Next Questions (Require More Data)
1. **Expansion:** How does performance scale with more orders?
2. **Temporal:** What patterns emerge with full month of data?
3. **Segmentation:** Do customer behaviors differ between SA and AE with larger samples?
4. **Quality:** Why is the $100 order missing month data?

## Immediate Actions Needed
1. **Collect more data** before meaningful analysis (target: 100+ orders)
2. **Fix the $100 order's** month assignment in source data
3. **Extend date range** to include actual Day 4 (Dec 4, 2025)
4. **Remove bootstrap analysis** until n>30 per country

## ETL Success Metrics
- **Rows Processed:** 5 orders successfully transformed
- **Join Coverage:** 100% of orders matched with user data
- **Schema Enforcement:** All required columns present and properly typed
- **Output Generated:** Analytics table ready for analysis (despite small size)