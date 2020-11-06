use project_1;


select sum(occurences) over(partition by reftype order by occurences rows between UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from clickstream;

create table wikiuse as 
select refType, sum(occurences) as totalOccurences from clickstream 
group by reftype;

create table wikiuseMain as 
select refType, sum(occurences) as totalOccurences from clickstream 
where referrer='Main_Page'
group by reftype;


select * from clickstreamlink where referrence='Main_Page'limit 10;
 /*Main_Page = 2379287*/
 
