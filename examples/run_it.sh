#!/bin/sh

trap 'kill_background' 2
kill_background() {
    kill $PID
    echo "got ctrl+c"
}

cp url_list url_temp
python ../Crawle/queue.py url_temp&
PID=$!
./SaveHandler.py PYROLOC://toad:7766/URLQueue 1
echo "URLs downloaded to output.gz."
echo "Press ctrl+c to kill queue"
wait
echo "Done"

