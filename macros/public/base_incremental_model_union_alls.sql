 {% macro base_incremental_model_union_alls(base_models, incremental_model) %}
  {% if is_incremental() %}
    {# Incremental build: Only include the incremental model #}
    
    SELECT * FROM {{ ref(incremental_model) }}
  {% else %}
    {# Initial build: Union all base models and the incremental model #}
    
      SELECT * FROM {{ ref(base_models[0]) }}
      {% for model in base_models[1:] %}
      UNION ALL
      SELECT * FROM {{ ref(model) }}
      {% endfor %}
      UNION ALL
      SELECT * FROM {{ ref(incremental_model) }}

  {% endif %}
{% endmacro %}

