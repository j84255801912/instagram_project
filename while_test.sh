#!/bin/bash

while true
do
    python dbmanager.py -m curl 2>>curling_error.log
done
