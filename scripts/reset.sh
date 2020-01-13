#!/bin/bash

rm -rf db storage
mkdir db storage storage/root
for i in {1..9}; do mkdir "storage/root${i}"; done

