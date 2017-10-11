#!/bin/sh

shuf /usr/share/dict/words | tr '[A-Z]' '[a-z]' | tr -dc '[a-z\n]' | gzip > tests/random_words.gz
zcat tests/random_words.gz | sort | gzip > tests/sorted_words.gz

echo -en '1\n2\n2\n3\n2\n' | gzip > tests/small.gz

