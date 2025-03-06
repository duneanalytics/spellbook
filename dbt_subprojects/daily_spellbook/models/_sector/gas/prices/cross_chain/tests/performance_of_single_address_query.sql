with address_records as (
    select * from {{model}}
    where address = 0xC94eBB328aC25b95DB0E0AA968371885Fa516215
    and address_prefix = CAST(SUBSTRING(LOWER(CAST(0xC94eBB328aC25b95DB0E0AA968371885Fa516215 AS VARCHAR)), 3, 2) AS VARCHAR)
)
select count(*) from address_records
where count(*) = 0