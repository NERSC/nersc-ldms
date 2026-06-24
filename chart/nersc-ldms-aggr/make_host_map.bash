#!/bin/bash

CONFIG_FILE=${1:-ldms_machine_config.json}

if [ ! -d "out_dir" ]; then mkdir -p out_dir; fi

# Read node type names from config and copy each host_map file
for NTYPE in $(python3 -c "import json; c=json.load(open('${CONFIG_FILE}')); print(' '.join(c.get('node_types',{}).keys()))"); do
    HOST_MAP_FILE="host_map.${NTYPE}.json"
    if [ -f "${HOST_MAP_FILE}" ]; then
        echo "Copying ${HOST_MAP_FILE} to out_dir"
        cp "${HOST_MAP_FILE}" out_dir
    else
        echo "WARNING: ${HOST_MAP_FILE} not found"
    fi
done
