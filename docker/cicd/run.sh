#!/bin/bash
# echo "****************************ENV*************************************"
# env
# echo "********************************************************************"
find . -type d \( -name ".pytest_cache" -o -name "__pycache__" \) -exec rm -r {} \+
flake8 && pytest -vv -p no:cacheprovider tests && bandit -lll -r .
