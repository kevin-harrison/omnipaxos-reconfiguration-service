#!/bin/bash

# Remove all but first configs
find . -name "*.conf" ! -name "*c1.conf" -delete
# Clear OmniPaxos persistant memory logs
rm -r logs
