#!/bin/sh

status=0
find ./tests -name '*.sh' ! -name '_*' | while read script; do
    $script || status=1
done

exit $status

