# Start all workers
java -jar seep-worker-0.1.jar --master.ip 127.0.0.1 --worker.port 3501 --data.port 5001 | sed 's/^/\[Worker 1]: /' &
java -jar seep-worker-0.1.jar --master.ip 127.0.0.1 --worker.port 3502 --data.port 5002 | sed 's/^/\[Worker 2]: /' &
java -jar seep-worker-0.1.jar --master.ip 127.0.0.1 --worker.port 3503 --data.port 5003 | sed 's/^/\[Worker 3]: /' & 
