import json
import os
import subprocess
from string import Template


def get_all_pages(endpoint_slug):
    items = []
    condition = True
    page = 1
    while condition:
        print(f"page: {page}")
        old_num_items = len(items)
        resp = json.loads(os.popen(f"""gh api \
          -H "Accept: application/vnd.github+json" \
          {endpoint_slug}{page}""").read())
        items = items + resp
        num_items = len(items)
        if num_items == old_num_items:
            condition = False
        else:
            page = page+1
        # 2 pages is enough :)
        if page == 2:
            condition = False
    return items


class TestPRs():
    def __init__(self):
        pass

    def get_shas(self):
        pulls_slug = '--method GET /repos/duneanalytics/spellbook/pulls -f per_page=100 -F state=open -f page='
        pulls = get_all_pages(pulls_slug)
        self.shas = [{'base': pull['base']['sha'], 'head': pull['head']['sha']} for pull in pulls]

    def generate_test_files(self):
        fetch_command = Template("git fetch origin $SHA")
        diff_command = Template('git diff $BASE_SHA..$HEAD_SHA -- ../../models/prices/prices_tokens.sql | grep "^\+ "')
        diff_file_command = Template('git diff $BASE_SHA..$HEAD_SHA -- ../../models/prices/prices_tokens.sql | grep "^\+ " > $FILE')

        for i, shas in enumerate(self.shas):
            # Get base
            fetch = fetch_command.substitute(SHA=shas['base'])
            os.system(fetch)

            # Get head
            fetch = fetch_command.substitute(SHA=shas['head'])
            os.system(fetch)

            # Get diff file
            cmd = diff_command.substitute(BASE_SHA=shas['base'], HEAD_SHA=shas['head'])
            res = subprocess.run(cmd, capture_output=True, shell=True).stdout.decode("utf-8")
            if res != '':
                print('hit')
                write_command = diff_file_command.substitute(BASE_SHA=shas['base'], HEAD_SHA=shas['head'], FILE=f"test_diffs_tokens/new_lines_{i}.txt")
                os.system(write_command)
            else:
                print('empty')
            os.system('git prune')

    def main(self):
        self.get_shas()
        self.generate_test_files()


test_prs = TestPRs()
test_prs.main()
