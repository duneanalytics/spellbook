{% macro bytea2numeric_v3() %}
    create or replace function bytea2numeric_v3(column_name STRING)
    returns STRING
    return
    with base_factors as (
        select
         conv(substring(lpad(column_name::string, 64, '0'),1,8),16,10)::decimal(38) as hex_5        -- 16^56 -> 16^63
        ,conv(substring(lpad(column_name::string, 64, '0'),9,14),16,10)::decimal(38) as hex_4      -- 16^42 -> 16^55
        ,conv(substring(lpad(column_name::string, 64, '0'),23,14),16,10)::decimal(38) as hex_3     -- 16^28 -> 16^41
        ,conv(substring(lpad(column_name::string, 64, '0'),37,14),16,10)::decimal(38) as hex_2     -- 16^14 -> 16^27
        ,conv(substring(lpad(column_name::string, 64, '0'),51,14),16,10)::decimal(38) as hex_1     -- 16^0 -> 16^13
        --10^17
        ,pow(10,17)::decimal(38) as pow_10_17
        --16^14
        ,'72057594037927936'::decimal(38) as base_14_1
        --16^28
        ,substring('5192296858534827628530496329220096',1,17)::decimal(38) as base_28_2
        ,substring('5192296858534827628530496329220096',18,17)::decimal(38) as base_28_1
        --16^42
        ,substring('374144419156711147060143317175368453031918731001856',1,17)::decimal(38) as base_42_3
        ,substring('374144419156711147060143317175368453031918731001856',18,17)::decimal(38) as base_42_2
        ,substring('374144419156711147060143317175368453031918731001856',35,17)::decimal(38) as base_42_1
        --16^56
        ,substring('26959946667150639794667015087019630673637144422540572481103610249216',1,17)::decimal(38) as base_56_4
        ,substring('26959946667150639794667015087019630673637144422540572481103610249216',18,17)::decimal(38) as base_56_3
        ,substring('26959946667150639794667015087019630673637144422540572481103610249216',35,17)::decimal(38) as base_56_2
        ,substring('26959946667150639794667015087019630673637144422540572481103610249216',52,17)::decimal(38) as base_56_1
    )

    --compose all factors together in the correct decimal(38)s
    , decimals as (
    select
        left(lpad(hex_5 * base_56_4,34,'0'),17)::decimal(38) as dec_3
        ,(  left(lpad(hex_3 * base_28_2,34,'0'),17)::decimal(38)
            + left(lpad(hex_4 * base_42_2,34,'0'),17)::decimal(38) + (hex_4 * base_42_3)
            + left(lpad(hex_5 * base_56_2,34,'0'),17)::decimal(38) + (hex_5 * base_56_3) + right(hex_5 * base_56_4,17)::decimal(38)*pow_10_17
         )::decimal(38) as dec_2
        ,(  hex_1
            + (hex_2 * base_14_1)
            + (hex_3 * base_28_1) + right(hex_3 * base_28_2,17)::decimal(38)*pow_10_17
            + (hex_4 * base_42_1) + right(hex_4 * base_42_2,17)::decimal(38)*pow_10_17
            + (hex_5 * base_56_1) + right(hex_5 * base_56_2,17)::decimal(38)*pow_10_17
         )::decimal(38) as dec_1
        from base_factors
    )

    --handle carry overs from dec1 -> dec2 and dec2 -> dec3
    , carries as (
        select
        dec_3 + left(lpad(dec_2,35,'0'),1)::decimal(38) as dec_3
        ,right(dec_2,34) as dec_2
        ,dec_1
        from (
            select
                dec_3
                ,dec_2 + left(lpad(dec_1,35,'0'),1)::decimal(38) as dec_2
                ,right(dec_1,34) as dec_1
            from decimals
        )
    )
    , result as (
        select
            ltrim('0',lpad(dec_3,10,'0') || lpad(dec_2,34,'0') || lpad(dec_1,34,'0')) as dec_string
        from carries
    )
    select
        case when dec_string = '' then '0' else dec_string end as out
    from result;
{% endmacro %}
