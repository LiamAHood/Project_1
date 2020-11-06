use project_1;

create external table AUS2
	(Site String, Views Int)
	Row format delimited
	Fields terminated by '\t'
	location '/user/lhood/project_1_data/pageviewAUSTable2';
	
Load data inpath '/user/lhood/project_1_data/pageviewAUSu2/part-r-00000' into table AUS2;

create external table USA2
	(Site String, Views Int)
	Row format delimited
	Fields terminated by '\t'
	location '/user/lhood/project_1_data/pageviewUSATable2';
	
Load data inpath '/user/lhood/project_1_data/pageviewUSAu2/part-r-00000' into table USA2;

create external table GBR2
	(Site String, Views Int)
	Row format delimited
	Fields terminated by '\t'
	location '/user/lhood/project_1_data/pageviewGBRTable2';
	
Load data inpath '/user/lhood/project_1_data/pageviewGBRu2/part-r-00000' into table GBR2;
/*
select USA.site, USA.views/sum(USA.views) over () as USA_norm_views, GBR.views/sum(GBR.views) over () as GBR_norm_views, AUS.views/sum(AUS.views) over () as AUS_norm_views
from USA 
join AUS on USA.site = AUS.site
join GBR on USA.site = GBR.site
order by USA_norm_views desc
Limit 50;
*/

create table pageviewCountry as
select USA2.site, USA2.views, 100*(GBR2.views/sum(GBR2.views) over () - USA2.views/sum(USA2.views) over ())/(GBR2.views/sum(GBR2.views) over ()) as GBR_rel2_USA, 100*(AUS2.views/sum(AUS2.views) over () - USA2.views/sum(USA2.views) over ())/(AUS2.views/sum(AUS2.views) over ()) as AUS_rel2_USA
from USA2 
join AUS2 on USA2.site = AUS2.site
join GBR2 on USA2.site = GBR2.site
order by USA2.views desc
Limit 50;