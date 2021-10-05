# python_azure_utilities
This repo contains the python utilities for various azure services using Microsoft Azure python SDKs and OSS Sdks

## Python whl File Creation
python setup.py bdist_wheel

## Python pytest run command:
python -m pytest <filepath>

 ## Python pylint Command
   pylint --output-format=pylint_junit.JUnitReporter --ignore test --disable=C0116,C0115,C0114,R0903 --extension-pkg-whitelist pyodbc ./code/deployment/python_utility_package/utility_package > utility_package-lint-testresults.xml

 ## Code Coverage Commands
		a. coverage run -m pytest ./test/
		b. coverage report -m
