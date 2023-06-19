## OCR Aggregate

To distill ocr data to weekly or monthly aggregation, use the following as a reference

```sql
SELECT 
  'ethereum' as blockchain,
  date_trunc('week', block_date),
  admin_address,
  operator_name,
  SUM(token_amount) as token_amount,
  SUM(usd_amount) as usd_amount
FROM 
  chainlink_ethereum_ocr_reward_daily
GROUP BY
  2, 3
ORDER BY
  2, 3
```