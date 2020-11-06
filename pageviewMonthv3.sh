#!/bin/bash
echo "starting adding pageviews on September 1-5 to HDFS"
mkdir hadoop-3.2.1/pageviewMonthv3
hdfs dfs -mkdir project_1_data/pageviewMonthv3
hdfs dfs -mkdir project_1_data/pageview5daysv3

#wget -P hadoop-3.2.1/pageviewMonthv3 https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{01..30}-{00..23}0000.gz
#gunzip hadoop-3.2.1/pageviewMonthv3/pageviews-202009{01..30}-{00..23}0000.gz
hdfs dfs -put hadoop-3.2.1/pageviewMonthv3/pageviews-202009{01..30}-{00..23}0000 project_1_data/pageviewMonthv3/
rm hadoop-3.2.1/pageviewMonthv3/pageviews-202009{01..30}-{00..23}0000
echo "starting mapreduce"
hadoop jar hadoop-3.2.1/ScalaProjects/pageview_count/target/scala-2.13/pageview_count-assembly-0.1.jar /user/lhood/project_1_data/pageviewMonthv3 /user/lhood/project_1_data/pageviewMonthResultUv3
hdfs dfs -ls project_1_data

echo "reordering by views"
hadoop jar hadoop-3.2.1/ScalaProjects/sortbyvalue/target/scala-2.13/sortbyvalue-assembly-0.1.jar /user/lhood/project_1_data/pageviewMonthResultUv3/part-r-00000 /user/lhood/project_1_data/pageviewMonthResultv3
hdfs dfs -ls project_1_data
hdfs dfs -tail project_1_data/pageviewMonthResultsv3/part-r-00000
