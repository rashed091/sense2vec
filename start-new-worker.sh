#!/usr/bin/env bash

FILE_COUNT="$(find /home/newscred/flask-app/flask-app/uploads -mmin -7 | wc -l)"

if [ $FILE_COUNT -eq 0 ]; then
    bash /home/newscred/Workspace/flask-app/flask-app/launch.sh
fi
