#!/bin/bash

if [ ! -d "out_dir" ]; then mkdir -p out_dir; fi
cp host_map.slurm-cluster.json out_dir 
