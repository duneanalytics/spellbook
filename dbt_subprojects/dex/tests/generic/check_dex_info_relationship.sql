{% test check_dex_info_relationship(model) %}

select distinct m.project as missing_dex_info_project
from {{ model }} m
  left join {{ ref('dex_info') }} di on m.project = di.project
where di.project is null

{% endtest %}
