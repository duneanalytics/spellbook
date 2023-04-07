import argparse
import difflib

# Defaults use first test case from list of spellbook failures
#models/aave/ethereum/aave_ethereum_votes.translated.sql:
# error: line 8:47: Cannot apply operator: UINT256 / double
# Assuming single line

parser = argparse.ArgumentParser(description="Map spells to their compiled SQL")
parser.add_argument('file', nargs='?', type=str, default="/models/aave/ethereum/aave_ethereum_votes.sql")
parser.add_argument('start', nargs='?', type=int, default=8, help="Starting line number on compiled SQL")
parser.add_argument('end', nargs='?', type=int, default=9, help="Ending line number on compiled SQL")
args = parser.parse_args()

SPELL_PATH = args.file
COMPILED_PATH = f"/target/compiled/spellbook/{SPELL_PATH}"
START = args.start
END = args.end


class LineMapper:
    def __init__(self,
                 spell_path,
                 compiled_path,
                 start,
                 end):
        self.spell_path = f"../{spell_path}"
        self.compiled_path = f"../{compiled_path}"
        self.start = start
        self.end = end

    def get_mapping(self):
        # Read the contents of the two files
        with open(self.spell_path, 'r') as f1:
            self.spell = f1.readlines()

        with open(self.compiled_path, 'r') as f2:
            self.compiled = f2.readlines()

        # Create a SequenceMatcher object
        matcher = difflib.SequenceMatcher(None, self.compiled, self.spell)

        # Get the opcodes that describe the changes between the two files
        opcodes = matcher.get_opcodes()

        # Map line numbers between the two files
        file1_line_numbers = []
        file2_line_numbers = []
        currline = 0
        for opcode in opcodes:
            if opcode[0] == 'equal':
                # Common lines
                for i, j in list(zip(range(opcode[1], opcode[2]),
                                     range(opcode[3], opcode[4]))):
                    file1_line_numbers.append(i)
                    file2_line_numbers.append(j)
            elif opcode[0] in {'insert', 'replace'}:
                # Lines added or changed in file2
                for i in range(opcode[3], opcode[4]):
                    file1_line_numbers.append(None)
                    file2_line_numbers.append(i)
            elif opcode[0] in {'delete', 'replace'}:
                # Lines deleted or changed in file1
                for i in range(opcode[1], opcode[2]):
                    file1_line_numbers.append(i)
                    file2_line_numbers.append(None)
        self.mapping = list(zip(file1_line_numbers, file2_line_numbers))

    def return_compiled_lines(self):
        return self.compiled[self.start:self.end]

    def return_spell_lines(self):
        map = [val for key, val in self.mapping if key == self.start]
        assert len(map) == 1, f"Expected 1 line, got {len(map)}"
        return self.spell[map[0]:map[0] + self.end - self.start]

    def main(self):
        self.get_mapping()
        compiled_lines = self.return_compiled_lines()
        spell_lines = self.return_spell_lines()
        assert compiled_lines == spell_lines, f"Lines don't match:\n{compiled_lines}\n{spell_lines}"
        return self.return_spell_lines()

