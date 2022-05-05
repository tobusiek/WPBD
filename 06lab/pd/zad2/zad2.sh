#!/bin/bash
python3 delete_dir.py
python3 mkdir.py
/usr/local/hadoop/bin/hdfs dfs -put ./opowiesc.txt /mojkatalog/input
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.2.jar -files mapper.py,reducer.py -mapper "python3 mapper.py" -reducer "python3 reducer.py" -input /mojkatalog/input/opowiesc.txt -output /mojkatalog/output
python3 download.py