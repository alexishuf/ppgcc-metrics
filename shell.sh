#!/bin/bash
echo "${0%/*}"
test -d build || make
source build/env/bin/activate
ipython -i shell.py
