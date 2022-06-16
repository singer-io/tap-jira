import os
from tap_tester.logger import LOGGER


potential_paths = [
    'tests/',
    '../tests/'
    'tap-square/tests/',
    '../tap-square/tests/',
]


def go_to_tests_directory():
    for path in potential_paths:
        if os.path.exists(path):
            os.chdir(path)
            return os.getcwd()
    raise NotImplementedError("This check cannot run from {}".format(os.getcwd()))

##########################################################################
### TEST
##########################################################################


LOGGER.info("Acquiring path to tests directory.")
cwd = go_to_tests_directory()

LOGGER.info("Reading in filenames from tests directory.")
files_in_dir = os.listdir(cwd)

LOGGER.info("Dropping files that are not of the form 'test_<feature>.py'.")
test_files_in_dir = [fn for fn in files_in_dir if fn.startswith('test_') and fn.endswith('.py')]

LOGGER.info("Dropping test_client.py from test files.")
if 'test_client.py' in test_files_in_dir:
    test_files_in_dir.remove('test_client.py')

LOGGER.info("Files found: {}".format(test_files_in_dir))

LOGGER.info("Reading contents of circle config.")
with open(cwd + "/../.circleci/config.yml", "r") as config:
    contents = config.read()

LOGGER.info("Parsing circle config for run blocks.")
runs = contents.replace(' ', '').replace('\n', '').split('-run:')

LOGGER.info("Verify all test files are executed in circle...")
tests_not_found = set(test_files_in_dir)
for filename in test_files_in_dir:
    LOGGER.info("\tVerifying {} is running in circle.".format(filename))
    if any([filename in run for run in runs]):
        tests_not_found.remove(filename)
assert tests_not_found == set(), "The following tests are not running in circle:\t{}".format(tests_not_found)
LOGGER.info("\t SUCCESS: All tests are running in circle.")
