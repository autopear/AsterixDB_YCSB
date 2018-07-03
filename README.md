Run the python script [load_run.py](scripts/load_run.py) with Python 2 or Python 3.

The script was written and tested usign Python 3, but it should be working fine with Python 2.
You must have package `requests` installed for SQL++ command to run.

Edit the parameters in the script for your experiment.

By default, tasks will be run in order:
1. Load data for 8 hours, keys are hashed so they are in random order.
2. Read only for 2 hours
3. 95% read and 5% inserts for 2 hours.
3. 50% read and 50% inserts for 2 hours.

Some pre-defined policies are there, no-merge, constant and prefix.

If you want to restart the database between each load and run, you may modify variable `asterixdb` and functions `start_server()`, `stop_server()`. Note that you should increase the timeout value in `start-sample-cluster.sh` and `stop-sample-cluster.sh` accordingly.

You can find some example logs under [example_logs](example_logs).
