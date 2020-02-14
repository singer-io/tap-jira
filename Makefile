.DEFAULT_GOAL := test

test: pylint
	nosetests tests/unittests

pylint:
	pylint tap_jira -d missing-docstring,too-many-instance-attributes,too-few-public-methods,unsupported-assignment-operation,dangerous-default-value,unsupported-membership-test,unsubscriptable-object
