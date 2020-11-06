#!/bin/bash
echo "starting adding pageviews on September 1-5 to HDFS"
#mkdir hadoop-3.2.1/pageviewMonth
wget -P hadoop-3.2.1/pageviewMonth https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{01..05}-{00..23}0000.gz
gunzip hadoop-3.2.1/pageviewMonth/pageviews-202009{01..05}-{00..23}0000.gz
hdfs dfs -mkdir project_1_data/pageviewMonth
hdfs dfs -mkdir project_1_data/pageview5days

hdfs dfs -put hadoop-3.2.1/pageviewMonth/pageviews-202009{01..05}-{00..23}0000 project_1_data/pageview5days/
rm hadoop-3.2.1/pageviewMonth/pageviews-202009{01..05}-{00..23}0000
echo "starting first mapreduce"
hadoop jar hadoop-3.2.1/ScalaProjects/pageview_count/target/scala-2.13/pageview_count-assembly-0.1.jar /user/lhood/project_1_data/pageview5days /user/lhood/project_1_data/pageviewMonthResultTemp
hdfs dfs -mv project_1_data/pageviewMonthResultTemp/part-r-00000 project_1_data/pageviewMonthResultTemp/days_1-5
hdfs dfs -mv project_1_data/pageviewMonthResultTemp/days_1-5 project_1_data/pageviewMonth
hdfs dfs -rm -r project_1_data/pageviewMonthResultTemp
hdfs dfs -rm project_1_data/pageview5days/*

wget -P hadoop-3.2.1/pageviewMonth https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{06..10}-{00..23}0000.gz
gunzip hadoop-3.2.1/pageviewMonth/pageviews-202009{06..10}-{00..23}0000.gz
hdfs dfs -put hadoop-3.2.1/pageviewMonth/pageviews-202009{06..10}-{00..23}0000 project_1_data/pageview5days/
rm hadoop-3.2.1/pageviewMonth/pageviews-202009{06..10}-{00..23}0000
echo "starting first mapreduce"
hadoop jar hadoop-3.2.1/ScalaProjects/pageview_count/target/scala-2.13/pageview_count-assembly-0.1.jar /user/lhood/project_1_data/pageview5days /user/lhood/project_1_data/pageviewMonthResultTemp
hdfs dfs -ls project_1_data
hdfs dfs -mv project_1_data/pageviewMonthResultTemp/part-r-00000 project_1_data/pageviewMonthResultTemp/days_6-10
hdfs dfs -mv project_1_data/pageviewMonthResultTemp/days_6-10 project_1_data/pageviewMonth
hdfs dfs -rm -r project_1_data/pageviewMonthResultTemp
hdfs dfs -rm project_1_data/pageview5days/*

wget -P hadoop-3.2.1/pageviewMonth https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{11..15}-{00..23}0000.gz
gunzip hadoop-3.2.1/pageviewMonth/pageviews-202009{11..15}-{00..23}0000.gz
hdfs dfs -put hadoop-3.2.1/pageviewMonth/pageviews-202009{11..15}-{00..23}0000 project_1_data/pageview5days/
rm hadoop-3.2.1/pageviewMonth/pageviews-202009{11..15}-{00..23}0000
echo "starting first mapreduce"
hadoop jar hadoop-3.2.1/ScalaProjects/pageview_count/target/scala-2.13/pageview_count-assembly-0.1.jar /user/lhood/project_1_data/pageview5days /user/lhood/project_1_data/pageviewMonthResultTemp
hdfs dfs -ls project_1_data
hdfs dfs -mv project_1_data/pageviewMonthResultTemp/part-r-00000 project_1_data/pageviewMonthResultTemp/days_11-15
hdfs dfs -mv project_1_data/pageviewMonthResultTemp/days_11-15 project_1_data/pageviewMonth
hdfs dfs -rm -r project_1_data/pageviewMonthResultTemp
hdfs dfs -rm project_1_data/pageview5days/*

wget -P hadoop-3.2.1/pageviewMonth https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{16..20}-{00..23}0000.gz
gunzip hadoop-3.2.1/pageviewMonth/pageviews-202009{16..20}-{00..23}0000.gz
hdfs dfs -put hadoop-3.2.1/pageviewMonth/pageviews-202009{16..20}-{00..23}0000 project_1_data/pageview5days/
rm hadoop-3.2.1/pageviewMonth/pageviews-202009{16..20}-{00..23}0000
echo "starting first mapreduce"
hadoop jar hadoop-3.2.1/ScalaProjects/pageview_count/target/scala-2.13/pageview_count-assembly-0.1.jar /user/lhood/project_1_data/pageview5days /user/lhood/project_1_data/pageviewMonthResultTemp
hdfs dfs -ls project_1_data
hdfs dfs -mv project_1_data/pageviewMonthResultTemp/part-r-00000 project_1_data/pageviewMonthResultTemp/days_16-20
hdfs dfs -mv project_1_data/pageviewMonthResultTemp/days_16-20 project_1_data/pageviewMonth
hdfs dfs -rm -r project_1_data/pageviewMonthResultTemp
hdfs dfs -rm project_1_data/pageview5days/*

wget -P hadoop-3.2.1/pageviewMonth https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{21..25}-{00..23}0000.gz
gunzip hadoop-3.2.1/pageviewMonth/pageviews-202009{21..25}-{00..23}0000.gz
hdfs dfs -put hadoop-3.2.1/pageviewMonth/pageviews-202009{21..25}-{00..23}0000 project_1_data/pageview5days/
rm hadoop-3.2.1/pageviewMonth/pageviews-202009{21..25}-{00..23}0000
echo "starting first mapreduce"
hadoop jar hadoop-3.2.1/ScalaProjects/pageview_count/target/scala-2.13/pageview_count-assembly-0.1.jar /user/lhood/project_1_data/pageview5days /user/lhood/project_1_data/pageviewMonthResultTemp
hdfs dfs -ls project_1_data
hdfs dfs -mv project_1_data/pageviewMonthResultTemp/part-r-00000 project_1_data/pageviewMonthResultTemp/days_21-25
hdfs dfs -mv project_1_data/pageviewMonthResultTemp/days_21-25 project_1_data/pageviewMonth
hdfs dfs -rm -r project_1_data/pageviewMonthResultTemp
hdfs dfs -rm project_1_data/pageview5days/*

wget -P hadoop-3.2.1/pageviewMonth https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{26..30}-{00..23}0000.gz
gunzip hadoop-3.2.1/pageviewMonth/pageviews-202009{26..30}-{00..23}0000.gz
hdfs dfs -put hadoop-3.2.1/pageviewMonth/pageviews-202009{26..30}-{00..23}0000 project_1_data/pageview5days/
rm hadoop-3.2.1/pageviewMonth/pageviews-202009{26..30}-{00..23}0000
echo "starting first mapreduce"
hadoop jar hadoop-3.2.1/ScalaProjects/pageview_count/target/scala-2.13/pageview_count-assembly-0.1.jar /user/lhood/project_1_data/pageview5days /user/lhood/project_1_data/pageviewMonthResultTemp
hdfs dfs -ls project_1_data
hdfs dfs -mv project_1_data/pageviewMonthResultTemp/part-r-00000 project_1_data/pageviewMonthResultTemp/days_26-30
hdfs dfs -mv project_1_data/pageviewMonthResultTemp/days_26-30 project_1_data/pageviewMonth
hdfs dfs -rm -r project_1_data/pageviewMonthResultTemp
hdfs dfs -rm project_1_data/pageview5days/*

hadoop jar hadoop-3.2.1/ScalaProjects/pageview_aggregate/target/scala-2.13/pageview_aggregate-assembly-0.1.jar /user/lhood/project_1_data/pageviewMonth /user/lhood/project_1_data/pageviewMonthResultU
hdfs dfs -ls project_1_data

echo "reordering by views"
hadoop jar hadoop-3.2.1/ScalaProjects/sortbyvalue/target/scala-2.13/sortbyvalue-assembly-0.1.jar /user/lhood/project_1_data/pageviewMonthResultU/part-r-00000 /user/lhood/project_1_data/pageviewMonthResult
#hdfs dfs -rm project_1_data/pageviewDay/*
hdfs dfs -ls project_1_data
hdfs dfs -tail project_1_data/pageviewMonthResults/part-r-00000
