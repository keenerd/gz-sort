#!/bin/sh

tput bold; echo "$0"; tput sgr0

true_md5="$(zcat tests/sorted_words.gz | tests/_hash.sh)"

./gz-sort -P 1 tests/random_words.gz tests/result.gz
test_md5="$(zcat tests/result.gz | tests/_hash.sh)"
if [ "$true_md5" != "$test_md5" ]; then
    tput setaf 1; tput rev; echo "ERROR - $0 (1 thread)"; tput sgr0
    exit 1
fi

./gz-sort -P 4 tests/random_words.gz tests/result.gz
test_md5="$(zcat tests/result.gz | tests/_hash.sh)"
if [ "$true_md5" != "$test_md5" ]; then
    tput setaf 1; tput rev; echo "ERROR - $0 (4 thread)"; tput sgr0
    exit 1
fi


true_md5="$(zcat tests/sorted_words.gz | uniq | tests/_hash.sh)"

./gz-sort -P 4 -u tests/random_words.gz tests/result.gz
test_md5="$(zcat tests/result.gz | tests/_hash.sh)"
if [ "$true_md5" != "$test_md5" ]; then
    tput setaf 1; tput rev; echo "ERROR - $0 (unique)"; tput sgr0
    exit 1
fi

