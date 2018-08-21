#!/bin/sh
uname=$(uname)
if [ -z "${uname##*BSD*}" ]; then
    md5 -q $@
else
    md5sum $@
fi
