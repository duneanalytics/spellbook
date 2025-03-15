# TON Specific Macros

TON blockchain has an unique way of storing data called ["Bag Of Cells"](https://docs.ton.org/v3/documentation/data-formats/tlb/cell-boc) (a.k.a. BoC).
It is used as a payload for messages and as an internal representation of all data structures. Handling of BoC in an analytical workflow is quite challenging since
most of the data is serialized with variable lengths fields and also bit-level granularity is widely used.

To overcome those challenges, the following macros are created:

## ton_boc_begin_parse

The entry point for any BoC parsing. In accepts a payload of type ``varbinary`` and parses the header of the BoC according to the [specification](https://github.com/ton-blockchain/ton/blob/24dc184a2ea67f9c47042b4104bbb4d82289fac1/crypto/tl/boc.tlb)
and returns a row with extracted fields. It is not supposed to be used directly, but rather as an input for other macros.

## ton_cell_load_cell

This macro is used to load a cell from the BoC. It accepts an output of ``ton_boc_begin_parse`` and returns two named fields:
* ``cell`` - a row with the cell binary data and auxiliary metadata
* ``cell_cursor`` - a row with the cursor to the next cell

Currently only the first cell (root cell) is supported.

``cell_cursor`` is a special entity used to track the current parsing position. It has two fields:
* ``bit_offset`` - the number of bits from the beginning of the cell that were already parsed
* ``ref_offset`` - the number of references that were already parsed (Not supported yet)

Note that ``ton_cell_load_cell`` must be called in a select clause and cannot be used as a parameter for other macros.

## ton_cell_preload_uint/ton_cell_preload_int

These macros are used to parse a uint/int value from the cell without changing the cursor position. Both macros accept the same parameters:
* ``cell`` - result from ``ton_cell_load_cell``
* ``cell_cursor`` - current cell cursor
* ``size`` - the number of bits to parse. Max size supported is 256.

Both macros return a value of type ``uint256`` or ``int256`` respectively.

## ton_cell_load_uint/ton_cell_load_int

These macros are used to parse a uint/int value from the cell and move the cursor to the next position. Macros accept the same parameters as ``ton_cell_preload_uint/ton_cell_preload_int``
with one additional parameter ``cast_row``. If set to ``true``, the macro returns a row without excplicit type casting (may be useful for complex 
statements with ``CASE ..WHEN ..ELSE``), otherwise it returns an extended ``cell_cursor``-like object with additional ``value`` field. All other ``ton_cell_load_*``
macros follow the same pattern.

## ton_cell_load_address

This macro is used to parse an address from the cell according to [the specification](https://github.com/ton-blockchain/ton/blob/master/crypto/block/block.tlb#L100-L110).
It accepts the same parameters as ``ton_cell_load_uint`` and returns an extended ``cell_cursor``: ``ROW(bit_offset bigint, ref_offset bigint, address varchar)``
Note that only ``addr_std`` is supported at the moment and ``Anycast`` is not supported yet.

## ton_cell_skip_bits

This macro is used to skip a certain number of bits in the cell without parsing them. It may be useful to parse multiple values from the same cell inside
a single select clause. See an example below:
```sql
format('0x%x',CAST({{ ton_cell_preload_uint('cell', 'cell_cursor', 32) }} as bigint)) AS opcode,
format('0x%x',CAST({{ ton_cell_preload_uint('cell', ton_cell_skip_bits('cell_cursor', 32), 64) }} as bigint)) AS query_id,
```

# Useful examples

Some parsing examples can be found in the project tables:
* [evaa_ton_supply](../../../models/evaa/ton/evaa_ton_supply.sql) - parsing of a single cell with uint/int/address data types