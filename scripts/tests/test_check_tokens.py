import os
import subprocess


def test_check_tokens():
    test_files = os.listdir('test_diffs_tokens')
    errs = []
    for test_file in test_files:
        cmd = f'python ../check_tokens.py --file_name test_diffs_tokens/{test_file}'
        err = subprocess.run(cmd, capture_output=True, shell=True).stderr.decode("utf-8")
        errs.append(err)