import json
import subprocess


def get_all_pages(endpoint_slug):
    items = []
    condition = True
    page = 1
    while condition:
        print(f"page: {page}")
        old_num_items = len(items)
        result = subprocess.run(
            ['gh', 'api', '-H', 'Accept: application/vnd.github+json',
             f'{endpoint_slug}{page}'],
            capture_output=True, text=True)
        resp = json.loads(result.stdout)
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
        for i, shas in enumerate(self.shas):
            # Get base
            subprocess.run(['git', 'fetch', 'origin', shas['base']])

            # Get head
            subprocess.run(['git', 'fetch', 'origin', shas['head']])

            # Get diff
            diff_result = subprocess.run(
                ['git', 'diff', f"{shas['base']}..{shas['head']}", '--',
                 '../../models/prices/prices_tokens.sql'],
                capture_output=True, text=True)
            # Filter lines starting with '+ '
            diff_lines = [line for line in diff_result.stdout.splitlines() if line.startswith('+ ')]
            if diff_lines:
                print('hit')
                with open(f"test_diffs_tokens/new_lines_{i}.txt", 'w') as f:
                    f.write('\n'.join(diff_lines) + '\n')
            else:
                print('empty')
            subprocess.run(['git', 'prune'])

    def main(self):
        self.get_shas()
        self.generate_test_files()


test_prs = TestPRs()
test_prs.main()
