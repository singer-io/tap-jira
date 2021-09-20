.DEFAULT_GOAL := test

test: pylint
	nosetests tests/unittests

pylint:
	pylint tap_jira -d '${PYLINT_DISABLE_LIST}'
