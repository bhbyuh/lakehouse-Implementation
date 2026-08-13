#!/bin/bash
# start.sh

# Run Jupyter Lab
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root &
JUPYTER_PID=$!

# Optional: Launch PySpark in background (for interactive shell)
# pyspark --master local[*] &

# Wait for Jupyter to exit
wait $JUPYTER_PID
