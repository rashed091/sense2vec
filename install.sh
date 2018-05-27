#!/usr/bin/env bash

pip install -r requirements.txt; python -m spacy download en;

#DIRECTORY="venv"

#if [ ! -d "$DIRECTORY" ]; then
#    cd $(pwd); virtualenv venv; source venv/bin/activate; pip install -r requirements.txt; python -m spacy download en;
#elif [ -d "$DIRECTORY" ]; then
#    cd $(pwd); source venv/bin/activate; python -m spacy download en;
#fi
