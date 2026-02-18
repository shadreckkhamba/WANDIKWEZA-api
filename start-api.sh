#!/bin/bash
# start-api.sh

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Get current day (1=Monday, 6=Saturday) and hour (24h format)
DAY=$(date +%u)
HOUR=$(date +%H)

# Run only Mon-Sat (1-6) and 6am-6pm (06-18)
if [ "$DAY" -ge 1 ] && [ "$DAY" -le 6 ] && [ "$HOUR" -ge 6 ] && [ "$HOUR" -lt 18 ]; then
    cd /home/ghii/indicator_api
    exec /home/ghii/indicator_api/venv/bin/gunicorn --bind 0.0.0.0:5000 run:app
else
    echo "API not allowed to run at this time."
    exit 0
fi
