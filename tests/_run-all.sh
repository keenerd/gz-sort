#!/bin/sh

status=0
while read script; do
    $script || status=1
done <<< "$(find ./tests -name '*.sh' ! -name '_*')"

exit $status

