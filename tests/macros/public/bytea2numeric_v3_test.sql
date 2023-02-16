WITH unit_tests as
(
    SELECT
        seed.hex
        ,seed.dec as expected
        ,bytea2numeric_v3(hex) as result
        from {{ ref('bytea2numeric_samples') }} seed
)
select * from unit_tests
where result != expected
