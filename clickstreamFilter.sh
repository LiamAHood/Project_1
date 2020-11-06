#!/bin/bash
echo "starting adding clickstream data for September 2020"
#mkdir hadoop-3.2.1/clickstream
#wget -P hadoop-3.2.1/clickstream https://dumps.wikimedia.org/other/clickstream/2020-09/clickstream-enwiki-2020-09.tsv.gz
#gunzip hadoop-3.2.1/clickstream/clickstream-enwiki-2020-09.tsv.gz
#hdfs dfs -mkdir project_1_data/clickstream
#hdfs dfs -put hadoop-3.2.1/clickstream/clickstream-enwiki-2020-09.tsv project_1_data/clickstream/

echo "starting mapreduce"
hadoop jar hadoop-3.2.1/ScalaProjects/clickstream_link_filter/target/scala-2.13/clickstream_link_filter-assembly-0.1.jar /user/lhood/project_1_data/clickstream /user/lhood/project_1_data/clickstreamLinkU

echo "reordering by views"
hadoop jar hadoop-3.2.1/ScalaProjects/sortbyvalue/target/scala-2.13/sortbyvalue-assembly-0.1.jar /user/lhood/project_1_data/clickstreamLinkU /user/lhood/project_1_data/clickstreamLink
hdfs dfs -tail project_1_data/clickstreamLink/part-r-00000
