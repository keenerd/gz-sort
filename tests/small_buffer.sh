#!/bin/sh

tput bold; echo "$0"; tput sgr0

true_md5="$(zcat tests/sorted_words.gz | md5sum)"

./gz-sort -S 1k tests/random_words.gz tests/result.gz
test_md5="$(zcat tests/result.gz | md5sum)"
if [ "$true_md5" != "$test_md5" ]; then
    tput setaf 1; tput rev; echo "ERROR - $0 (simple)"; tput sgr0
    exit 1
fi

./gz-sort -S 1k -P 1 tests/random_words.gz tests/result.gz
test_md5="$(zcat tests/result.gz | md5sum)"
if [ "$true_md5" != "$test_md5" ]; then
    tput setaf 1; tput rev; echo "ERROR - $0 (1 thread)"; tput sgr0
    exit 1
fi

./gz-sort -S 1k -P 4 tests/random_words.gz tests/result.gz
test_md5="$(zcat tests/result.gz | md5sum)"
if [ "$true_md5" != "$test_md5" ]; then
    tput setaf 1; tput rev; echo "ERROR - $0 (4 thread)"; tput sgr0
    exit 1
fi


true_md5="$(zcat tests/sorted_words.gz | uniq | md5sum)"

./gz-sort -S 1k -u tests/random_words.gz tests/result.gz
test_md5="$(zcat tests/result.gz | md5sum)"
if [ "$true_md5" != "$test_md5" ]; then
    tput setaf 1; tput rev; echo "ERROR - $0 (unique)"; tput sgr0
    exit 1
fi

