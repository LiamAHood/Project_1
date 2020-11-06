use project_1;
create external table pageviews
	(Site_Name String, Views Int)
	Row format delimited
	Fields terminated by '\t'
	/*Location '/user/lhood/project_1_data/pageviewsTable'*/
	tblproperties("skip.header.line.count"="1");
Load data inpath '/user/lhood/project_1_data/pageviewResults/part-r-00000' into table pageviews;

select * from pageviews
	order by views desc
	limit 50;
	
/* answer is Main_Page, Search, -, Jeffrey_Toobin