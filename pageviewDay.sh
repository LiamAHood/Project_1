#!/bin/bash
echo "starting adding pageviews on October 20 to HDFS"
mkdir hadoop-3.2.1/pageviewDay_10_20
wget -P hadoop-3.2.1/pageviewDay_10_20 https://dumps.wikimedia.org/other/pageviews/2020/2020-10/pageviews-20201020-{00..23}0000.gz
gunzip hadoop-3.2.1/pageviewDay_10_20/pageviews-20201020-{00..23}0000.gz
hdfs dfs -mkdir project_1_data/pageviewDay
hdfs dfs -put hadoop-3.2.1/pageviewDay_10_20/pageviews-20201020-{00..23}0000 project_1_data/pageviewDay/

echo "starting mapreduce"
hadoop jar hadoop-3.2.1/ScalaProjects/pageview_count/target/scala-2.13/pageview_count-assembly-0.1.jar /user/lhood/project_1_data/pageviewDay /user/lhood/project_1_data/pageviewDayResultU

echo "reordering by views"
hadoop jar hadoop-3.2.1/ScalaProjects/sortbyvalue/target/scala-2.13/sortbyvalue-assembly-0.1.jar /user/lhood/project_1_data/pageviewDayResultU /user/lhood/project_1_data/pageviewDayResults
hdfs dfs -rm project_1_data/pageviewDay/*
hdfs dfs -tail project_1_data/pageviewDayResults/part-r-00000

