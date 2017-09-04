#!/usr/bin/env bash

# remove all comments (from # to end of line) from file, and resulting empty lines
sed -i 's:#.*$::g' "$1"
sed -i '/^$/d' "$1"
