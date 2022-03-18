#!/bin/bash

size_small_objects=300k
size_medium_objects=5M
size_large_objects=15M
count_objects=150


for i in $(seq 1 $count_objects)
    do
	dd if=/dev/random of=./input/$RANDOM\_small bs=$size_small_objects count=1 status=none
	dd if=/dev/random of=./input/$RANDOM\_medium bs=$size_medium_objects count=1 status=none
	dd if=/dev/random of=./input/$RANDOM\_large bs=$size_large_objects count=1 status=none
    done
du -hd0 ./input/
