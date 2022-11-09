# Instructions to run

Make sure Spark is installed and zookeeper and kafka are running.
1. Run the stream.py in one terminal 
2. Run the read_stream.py in another terminal using 
```bash 
$SPARK_HOME/bin/spark-submit read_stream.py > output.log
```