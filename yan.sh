#!/usr/bin/env bash

# Check that virtual environment exists
#
if [[ ! -e "/opt/yan/bin/activate" ]]; then
    echo "CRITICAL: no virtual environment found under /opt/yan"
    exit 1
fi

# Activate virtualenv
source /opt/yan/bin/activate

# Execute vipbot
exec python -m yan.yan ${@:1}
