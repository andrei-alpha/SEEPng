# Kill all the workers
ps aux | grep seep-worker | grep -v grep | awk '{print $2}' | xargs kill -s 6
echo 'Killed all seep-workers.'

# Kill the master
ps aux | grep seep-master | grep -v grep | awk '{print $2}' | xargs kill -s 6
echo 'Killed seep-master'
