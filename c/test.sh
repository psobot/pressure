#!/bin/bash

cat /usr/share/dict/words | grep purple | ./put test_2 & 
./get test_2 | awk '{print length($1), $1}' | ./put test_3 &
./get test_3 | sort -n | ./put test_4 &
./get test_4 | tail -n 1 | ./put test_5 &
./get test_5 | cut -d " " -f 2 | ./put test_6 &
./get test_6 | cowsay -f tux