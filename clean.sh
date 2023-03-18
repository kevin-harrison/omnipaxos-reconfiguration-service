#!/bin/bash

# Remove all configs
find . -name "*.conf" -delete

# Clear OmniPaxos persistant memory logs
rm -r logs 2> /dev/null || true

# Create initial config files
printf "{
    config_id: 1,
    pid: 1,
    peers: [2],
    log_file_path: \"logs/\"
}" >> "./config/node1/c1.conf"

printf "{
    config_id: 1,
    pid: 2,
    peers: [1],
    log_file_path: \"logs/\"
}" >> "./config/node2/c1.conf"
