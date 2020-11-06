use project_1;

create external table clickstream
	(Referrer String, Site String, RefType String, Occurences Int)
	Row format delimited
	Fields terminated by '\t'
	location '/user/lhood/project_1_data/clickstreamTable';

Load data inpath '/user/lhood/project_1_data/clickstream/clickstream-enwiki-2020-09.tsv' into table clickstream;


INSERT Overwrite DIRECTORY '/user/hive/fromHC' ROW format delimited fields terminated by '/t'
select site, 100*occurences/(sum(occurences) over(partition by referrer)) as perc from clickstream where referrer='Hotel_California' order by occurences desc LIMIT 5;
/* Hotel_California_(Eagles_album) 16.0 clickthrough percent */
INSERT Overwrite DIRECTORY '/user/hive/fromHC_Ea' ROW format delimited fields terminated by '/t'
select site, 100*occurences/(sum(occurences) over(partition by referrer)) as perc from clickstream where referrer='Hotel_California_(Eagles_album)' order by occurences desc LIMIT 5;
/* The_Long_Run_(album) 18.4 clickthroughs percent */
INSERT Overwrite DIRECTORY '/user/hive/fromTLR_a' ROW format delimited fields terminated by '/t'
select site, 100*occurences/(sum(occurences) over(partition by referrer)) as perc from clickstream where referrer='The_Long_Run_(album)' order by occurences desc LIMIT 5;
/* Eagles_Live 24.3 clickthroughs percent */
INSERT Overwrite DIRECTORY '/user/hive/fromEL' ROW format delimited fields terminated by '/t'
select site, 100*occurences/(sum(occurences) over(partition by referrer)) as perc from clickstream where referrer='Eagles_Live' order by occurences desc LIMIT 5;
/* Eagles_Greatest_Hits,_Vol._2 54.3 clickthroughs percent */
INSERT Overwrite DIRECTORY '/user/hive/fromEGH' ROW format delimited fields terminated by '/t'
select site, 100*occurences/(sum(occurences) over(partition by referrer)) as perc from clickstream where referrer='Eagles_Greatest_Hits,_Vol._2' order by occurences desc LIMIT 5;
/* The_Very_Best_of_the_Eagles 69.3 clickthroughs percent */
INSERT Overwrite DIRECTORY '/user/hive/fromVBE' ROW format delimited fields terminated by '/t'
select site, 100*occurences/(sum(occurences) over(partition by referrer)) as perc from clickstream where referrer='The_Very_Best_of_the_Eagles' order by occurences desc LIMIT 5;
/* Hell_Freezes_Over 78.9 clickthroughs percent */
INSERT Overwrite DIRECTORY '/user/hive/fromHFO' ROW format delimited fields terminated by '/t'
select site, 100*occurences/(sum(occurences) over(partition by referrer)) as perc from clickstream where referrer='Hell_Freezes_Over' order by occurences desc LIMIT 5;
/* gets to .002 which is ~2 users if everyone clicked through to somewhere next leads
to Selected_Works:_1972–1999/16.74641148325359 which is a fractional person even
with the generous setup 
*/
