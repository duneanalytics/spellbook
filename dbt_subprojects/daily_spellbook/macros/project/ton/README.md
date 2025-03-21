# TON Specific Macros

TON blockchain has an unique way of storing data called ["Bag Of Cells"](https://docs.ton.org/v3/documentation/data-formats/tlb/cell-boc) (a.k.a. BoC).
It is used as a payload for messages and as an internal representation of all data structures. Handling of BoC in an analytical workflow is quite challenging since
most of the data is serialized with variable-length fields and also bit-level granularity is widely used.

To overcome those challenges, the following macros are created:

## ton_from_boc

The entry point for any BoC parsing. In accepts a payload of type ``varbinary`` and parses the header of the BoC according to the [specification](https://github.com/ton-blockchain/ton/blob/24dc184a2ea67f9c47042b4104bbb4d82289fac1/crypto/tl/boc.tlb). Also it accepts as a second parameter a list of macros to be applied to the parsed data.
Each macros may change the parsing state and push some results to the output. The output is returned as a SQL ``ROW`` with corresponding fields and data types.

## ton_begin_parse

This macro is used to init parsing state for the cell. It must be called first in the list of macros to start the parsing. It doesn't return any results to the output array.

## ton_load_uint

Accepts ``size`` parameter and ``as`` (field name) parameter and parses a uint value from the current cell. The maximum possible ``size`` is 256.
If ``size`` is less than 64, the result is returned as a ``bigint``, otherwise as a ``UINT256``.

## ton_load_int

The same as ``ton_load_uint`` but for int values. The maximum possible ``size`` is 128.

## ton_load_address

This macro is used to parse an address from the cell according to [the specification](https://github.com/ton-blockchain/ton/blob/master/crypto/block/block.tlb#L100-L110).
It accepts only one parameter - ``as`` (field name). The result is added to the output row with the name ``as`` and ``varchar`` type.
Note that only ``addr_std`` is supported at the moment and ``Anycast`` is not supported yet.

## ton_skip_bits

Allows to skip a certain number of bits in the cell without parsing them. Usefull when we don't need some fields (for example, one can skip the first 32 bits
of a message since it is already available in the ``opcode`` field).

## ton_skip_refs

The same as ``ton_skip_bits`` but for reference cells.

## ton_load_ref

Loads a reference cell from the current cursor position. Doesn't accept any parameters. Note that after loading a reference cell, one need to call ``ton_begin_parse`` again to start parsing the new cell.

## ton_restart_parse

This macro is used to restart parsing from the beginning of the bag of cells. It doesn't accept any parameters. It is useful when 
you are working with multiple reference cells and after parsing the first cell you need to switch to the next one - in this case you need to call ``ton_restart_parse``,
skip the first ref with ``ton_skip_refs`` and then call ``ton_load_ref`` again to start parsing the next cell.

# Usage examples

Typical usage should be like this:

```sql
WITH parse_results AS (    
    SELECT {{ ton_from_boc('boc', 
    [
    ton_begin_parse(),
    ton_skip_bits(32),
    ton_load_uint(64, 'query_id'),
    ton_load_address('owner_address')
    ]) }} as result 
    FROM test_data
)
SELECT
    result.query_id,
    result.owner_address AS owner_address
FROM test_results
```


Some parsing examples can be found in the project tables:
* [evaa_ton_withdraw](../../../models/evaa/ton/evaa_ton_withdraw.sql) - parsing of a cell with a reference to another cell
