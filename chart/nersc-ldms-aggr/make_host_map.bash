#!/bin/bash

HOST_MAP_FILE=${1:-host_map.r7525.json}

if [ ! -d "out_dir" ]; then mkdir -p out_dir; fi
cp "${HOST_MAP_FILE}" out_dir
