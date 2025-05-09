{% macro tesseract_cell_types(
        blockchain = null
    )
%}

{% if blockchain == 'avalanche_c' %}

{{
    return([
        'YakSwapCell',
        'DexalotSimpleSwapCell'
    ])
}}

{% endif %}

{% endmacro %}