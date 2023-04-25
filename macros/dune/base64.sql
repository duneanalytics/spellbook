{%- macro base64(data) -%}
  {%- set CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/" -%}

  {%- set char_range = range(256) -%}
  {%- set char_dict = {} -%}
  {%- for char_code in char_range -%}
      {%- set char = "%c" % char_code -%}
      {%- set binary = '{:0>8b}'.format(char_code) -%}
      {%- set char_dict = char_dict.update({char: binary}) -%}
  {%- endfor -%}

  {%- set override = 0 -%}
  {%- if (data | length) % 3 != 0 -%}
    {%- set override = (data | length) + 3 - (data | length) % 3 - (data | length) -%}
  {%- endif -%}
  {%- set data = data ~ "\x00" * override -%}

  {%- set threechunks = data | batch(3) -%}

  {%- set binstring = namespace(value="") -%}
  {%- for chunk in threechunks -%}
    {%- for x in chunk -%}
      {%- set binstring.value = binstring.value ~ char_dict.get(x) -%}
    {%- endfor -%}
  {%- endfor -%}

  {%- set sixchunks = binstring.value | batch(6) -%}

  {%- set outstring = namespace(value="") -%}
  {%- for element in sixchunks -%}
    {%- set outstring.value = outstring.value ~ CHARS[(element | join("") | int(base=2))] -%}
  {%- endfor -%}

  {%- set outstring.value = outstring.value[:-override] ~ "=" * override -%}
  {{ outstring.value }}
{%- endmacro -%}