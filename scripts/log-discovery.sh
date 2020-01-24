#!/bin/bash

sudo tcpdump -i enp3s0 -A -s0 port 12345 and ether broadcast

