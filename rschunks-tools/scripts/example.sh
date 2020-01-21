#!/bin/bash

./tools.py reset
filename=$(./tools.py make-file "10M")
./cli.py init "./storage/${filename}"
./tools.py mutate-file --position 5000000 --size 10 "./storage/${filename}"
./cli.py update "./storage/${filename}"
diff -u --color metadata/${filename}/chunks-prev.json "metadata/${filename}/chunks.json"
