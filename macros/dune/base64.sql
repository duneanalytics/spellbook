{% macro base64(data) %}
  {% set CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/" %}
  
  {% set override = 0 %}
  {% if (data | length) % 3 != 0 %}
    {% set override = (data | length) + 3 - (data | length) % 3 - (data | length) %}
  {% endif %}
  {% set data = data ~ "\x00" * override %}
  
  {% set threechunks = data | batch(3) %}
  
  {% set binstring = "" %}
  {% for chunk in threechunks %}
    {% for x in chunk %}
      {% set binstring = binstring ~ (x | int | format('{:0>8b}')) %}
    {% endfor %}
  {% endfor %}
  
  {% set sixchunks = binstring | batch(6) %}
  
  {% set outstring = "" %}
  {% for element in sixchunks %}
    {% set outstring = outstring ~ CHARS[int(element, 2)] %}
  {% endfor %}
  
  {% set outstring = outstring[:-override] ~ "=" * override %}
  {{ outstring }}
{% endmacro %}