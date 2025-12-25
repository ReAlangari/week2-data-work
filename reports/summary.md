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

## Next Ques