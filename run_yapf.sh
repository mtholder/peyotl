#!/bin/bash
echo setup.py > dev/to-format-list.txt
for d in dev doc examples extras peyotl scripts standalone_tests tutorials
do
    find $d -name "*.py" >> dev/to-format-list.txt
done
for f in $(cat dev/to-format-list.txt)
do
    echo "formatting $f"
    yapf --style=google -i "$f"
done
