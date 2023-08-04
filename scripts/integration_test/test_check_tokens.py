import os
import subprocess


def test_check_tokens():
    test_files = os.listdir('test_diffs_tokens')
    errs = []
    for test_file in test_files:
        cmd = f'python ../check_tokens.py --file_name test_diffs_tokens/{test_file}'
        err = subprocess.run(cmd, capture_output=True, shell=True).stderr.decode("utf-8")
        errs.append(err)
    filter_empty_errs = [err for err in errs if err != '']
    filter_assetion_errs = [err for err in filter_empty_errs if 'raise Exception(f"{exceptions} exception/s' not in err]
    assert len(filter_assetion_errs) == 0
