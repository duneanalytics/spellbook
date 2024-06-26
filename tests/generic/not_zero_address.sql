select
    *
from {{ model }}
where {{column_name }} != 0x0000000000000000000000000000000000000000
