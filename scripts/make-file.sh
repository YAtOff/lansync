#!/bin/bash

size=$1

head -c "$size" </dev/urandom
