from scripts.token_checker import TokenChecker

with open('new_lines.txt') as f:
    new_lines = f.read().strip().split('\n')
new_lines = [new_line.lstrip('+').strip() for new_line in new_lines]
exceptions = 0
for new_line in new_lines:
    try:
        checker = TokenChecker(new_line=new_line)
        checker.validate_token()
    except Exception as err:
        exceptions+=1
        print(err)
if exceptions > 0:
    raise Exception(f"{exceptions} exception/s. Review logs for details. Some could be due simply to missing data from API.")