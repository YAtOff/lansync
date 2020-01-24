#!/bin/bash

rm -rf db storage log
mkdir db storage storage/root log
for i in {1..9}; do mkdir "storage/root${i}"; done

