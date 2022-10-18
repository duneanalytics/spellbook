-- Check for multiple addresses

SELECT address
, COUNT(*)
FROM {{ ref('addresses_events_ethereum_first_funded_by') }}
HAVING COUNT(*) > 1