{% macro to_wrapped_native_token(
        blockchain,    
        from_column_name,
        to_column_name    
    )
%}
{% if blockchain == 'ethereum' %}
CASE 
        WHEN {{from_column_name}} = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 
        ELSE {{from_column_name}}
    END AS {{to_column_name}}

{% elif blockchain == 'base' %}
CASE 
        WHEN {{from_column_name}} = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0x4200000000000000000000000000000000000006 
        ELSE {{from_column_name}}
    END AS {{to_column_name}}
{% endif %}
{% endmacro %}
