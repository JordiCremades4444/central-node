#!/bin/bash

# Navigate to the central-node directory to activate the virtual environment
cd /Users/jordicremades/Documents/repos/central-node

# Activate the virtual environment
source venv/bin/activate

# Navigate to the notebooks directory where the tracker is located
cd /Users/jordicremades/Documents/repos/central-node/notebooks/988_2024_9_ddp_tracker

# Run tracker.py script
python tracker.py

# Run the Jupyter notebook
jupyter nbconvert --to notebook --execute product_tracker.ipynb --output product_tracker_results.ipynb

# Move the results to the outputs folder
mv product_tracker_results.ipynb outputs/

# Print a message indicating completion
echo "All tracking is complete."