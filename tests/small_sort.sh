#!/bin/sh

tput bold; echo "$0"; tput sgr0
true_md5="$(zcat tests/small.gz | sort | tests/_hash.sh)"

./gz-sort tests/small.gz tests/result.gz
test_md5="$(zcat tests/result.gz | tests/_hash.sh)"
if [ "$true_md5" != "$test_md5" ]; then
    tput setaf 1; tput rev; echo "ERROR - $0"; tput sgr0
    exit 1
fi

