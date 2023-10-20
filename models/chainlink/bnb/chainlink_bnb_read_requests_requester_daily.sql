{{
  config(
    tags=['dunesql'],
    alias=alias('read_requests_requester_daily'),
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'requester_address']
  )
}}

SELECT
  'bnb' as blockchain,
  cast(date_trunc('day', read_requests_requester.date_start) AS date) AS date_start,
  MAX(cast(date_trunc('month', read_requests_requester.date_start) AS date)) AS date_month,
  read_requests_requester.requester_address as requester_address,
  MAX(read_requests_requester.requester_name) as requester_name,
  COUNT(*) as total_requests
FROM
  {{ref('chainlink_bnb_read_requests_requester')}} read_requests_requester
{% if is_incremental() %}
  WHERE 
  {{ incremental_predicate('date_start') }}
{% endif %}      
GROUP BY
  2, 4
ORDER BY
  2, 4
