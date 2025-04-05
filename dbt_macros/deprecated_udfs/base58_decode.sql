{% macro base58_decode(column_name) %}

concat(
    -- leading zeros generate leading zero bytes
    replace(regexp_extract({{ column_name }}, "^1*", 0), "1", "00"),
    array_join(reverse(
        aggregate(
            transform(
                regexp_extract_all({{ column_name }},"(.)"),
                x -> instr("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz", x)-1
            ),
            array(cast(0 as LONG)), -- output, starts at 0. Approximate final length: len(base58)*log(58, 2)/log(256,2) bytes, 4 in each long
            (current_outputs, current_char_index) -> -- accumulator, called for each letter-index in base58 string.
                -- ripple carry add
                aggregate(
                    current_outputs, -- go through all values in state, low to high. Keep track of carry
                    named_struct("carry_in", cast(current_char_index as long), "output", cast(array() as array<bigint>)), -- initialize carry_in as the value we want to add.
                    (state, current_output) -> -- for each part of the number, ripple-carry add. returns new number and carry_out
                        named_struct(
                            "carry_in", shiftrightunsigned(current_output * 58 + state["carry_in"], 32), -- carry_out is the new carry_in
                            "output", concat(state["output"],array((current_output * 58 + state["carry_in"]) & 4294967295))
                        ),
                    state -> -- finish, add final ripplecarry if present
                        case when state["carry_in"] > 0 then
                            concat(state["output"], array(state["carry_in"]))
                        else
                            state["output"]
                        end
                ),
            acc -> -- finish function, convert all to bytes
                transform(acc, (z, i) ->
                    -- if this is the last word, strip trailing zero bytes
                    case when i + 1 == cardinality(acc) then
                        case
                            when z >= 16777216 then lpad(hex(z), 8, "0")
                            when z >= 65536 then lpad(hex(z), 6, "0")
                            when z >= 256 then lpad(hex(z), 4, "0")
                            else lpad(hex(z), 2, "0")
                        end
                    -- for other words, generate 4 bytes always
                    else
                        lpad(hex(z), 8, "0")
                    end
                )
        )
    ), "")

)

{% endmacro %}
