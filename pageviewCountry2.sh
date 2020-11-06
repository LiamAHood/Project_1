#!/bin/bash
echo "starting adding pageviews on October 1-7 to HDFS"
#mkdir hadoop-3.2.1/pageviewCountry2
#wget -P hadoop-3.2.1/pageviewCountry2 https://dumps.wikimedia.org/other/pageviews/2020/2020-09/pageviews-202009{01..07}-{00..23}0000.gz
#gunzip hadoop-3.2.1/pageviewCountry2/pageviews-202009{01..07}-{00..23}0000.gz

#for country in "AUS" "USA" "GBR"
#do
#hdfs dfs -mkdir project_1_data/pageview"$country"Data2
#done

hdfs dfs -put hadoop-3.2.1/pageviewCountry2/pageviews-202009{01..07}-230000 project_1_data/pageviewAUSData2
hdfs dfs -put hadoop-3.2.1/pageviewCountry2/pageviews-202009{01..07}-{00..07}0000 project_1_data/pageviewAUSData2
hdfs dfs -put hadoop-3.2.1/pageviewCountry2/pageviews-202009{01..07}-{15..23}0000 project_1_data/pageviewUSAData2
hdfs dfs -put hadoop-3.2.1/pageviewCountry2/pageviews-202009{01..07}-{8..16}0000 project_1_data/pageviewGBRData2


for country in "AUS" "USA" "GBR"
do
hadoop jar hadoop-3.2.1/ScalaProjects/pageview_count/target/scala-2.13/pageview_count-assembly-0.1.jar /user/lhood/project_1_data/pageview"$country"Data2 /user/lhood/project_1_data/pageview"$country"u2
#hadoop jar hadoop-3.2.1/ScalaProjects/sortbyvalue/target/scala-2.13/sortbyvalue-assembly-0.1.jar /user/lhood/project_1_data/pageview"$country"U/part-r-00000 /user/lhood/project_1_data/pageview"$country"
done
