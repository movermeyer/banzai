export PYTHONPATH=.; py.test -v --ignore=build --cov banzai --cov visitors --cov nmmd --cov hercules --cov-report html --cov-config=.coveragerc
