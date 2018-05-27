#!/usr/bin/env bash

FILE_COUNT="$(find /home/newscred/sense2vec/uploads -mmin -7 | wc -l)"

if [ $FILE_COUNT -eq 0 ]; then
    bash /home/newscred/sense2vec/launch.sh
fi
