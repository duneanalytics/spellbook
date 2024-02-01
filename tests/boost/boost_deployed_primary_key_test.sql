with uniqueness_test AS (
SELECT 
CASE WHEN 
COUNT(DISTINCT boost_address) = COUNT(*) THEN 'true' ELSE 'false' END AS distinct_contracts
FROM {{ref('boost_deployed')}}
),

non_null AS (
SELECT *
FROM {{ref('boost_deployed')}} 
WHERE boost_address IS NOT NULL
)

select * from uniqueness_test
where distinct_contracts = 'false'
union all
select * from non_null