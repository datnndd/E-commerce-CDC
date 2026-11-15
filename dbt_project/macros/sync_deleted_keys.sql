{% macro sync_deleted_keys(target_relation, source_relation, keys) %}
  merge into {{ target_relation }} as target
  using (select distinct {% for key in keys %}{{ key }}{% if not loop.last %}, {% endif %}{% endfor %}
         from {{ source_relation }}) as source
  on {% for key in keys %}target.{{ key }} = source.{{ key }}{% if not loop.last %} and {% endif %}{% endfor %}
  when not matched by source then delete
{% endmacro %}
