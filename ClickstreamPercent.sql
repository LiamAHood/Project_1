use project_1;

create external table pageviewMonth
	(Site String, Views Int)
	Row format delimited
	Fields terminated by '\t'
	location '/user/lhood/project_1_data/pageviewMonthTable';

Load data inpath '/user/lhood/project_1_data/pageviewMonthResultU/part-r-00000' into table pageviewMonth;

create external table clickstreamLink 
	(Referrer String, Occurences Int)
	Row format delimited
	Fields terminated by '\t'
	location '/user/lhood/project_1_data/clickstreamLinkTable';
	
	Load data inpath '/user/lhood/project_1_data/clickstreamLinkU/solution' into table clickstreamLink;


create table clickstreamPercent as
	select clickstreamLink.Referrer, clickstreamLink.occurences, pageviewMonth.views, 100*(clickstreamLink.occurences/pageviewMonth.views) as perc 
	from clickstreamLink join pageviewMonth on (clickstreamLink.Referrer=pageviewMonth.site)
	order by perc desc;
	
	
