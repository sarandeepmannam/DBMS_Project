drop view if exists temp1 cascade;
drop view if exists temp2 cascade;

create view temp1 as
select T1.ref_by as X, T2.ref_by as Y, T3.ref_by as Z, T4.ref_by as W
from public.temp_citation as T1,public.temp_citation as T2,public.temp_citation as T3, public.temp_citation as T4
where T1.ref_to=T2.ref_by AND ((T2.ref_to=T3.ref_by AND T3.ref_to=T4.ref_by AND T4.ref_to=T1.ref_by) OR (T2.ref_to=T3.ref_by AND T3.ref_to=T1.ref_by));

create view temp2 as 
select T1.X as X,T1.Y as Y,T1.Z as Z,count(*) as num_count
from temp1 as T1,temp1 as T2
where (T1.X=T2.X AND T1.Y=T2.Y AND T1.Z=T2.Z) OR (T1.X=T2.X AND T1.Y=T2.Z AND T1.Z=T2.Y) OR(T1.X=T2.Y AND T1.Y=T2.X AND T1.Z=T2.Z) OR (T1.X=T2.Y AND T1.Y=T2.Z AND T1.Z=T2.X) OR(T1.X=T2.Z AND T1.Y=T2.X AND T1.Z=T2.Y) OR (T1.X=T2.Z AND T1.Y=T2.Y AND T1.Z=T2.X)
group by T1.X,T1.Y,T1.Z;

select T1.X,T1.Y,T1.Z,T1.num_count
from temp2 as T1,temp2 as T2
where (T1.X=T2.X AND T1.Y=T2.Y AND T1.Z=T2.Z) OR (T1.X=T2.X AND T1.Y=T2.Z AND T1.Z=T2.Y) OR(T1.X=T2.Y AND T1.Y=T2.X AND T1.Z=T2.Z) OR (T1.X=T2.Y AND T1.Y=T2.Z AND T1.Z=T2.X) OR(T1.X=T2.Z AND T1.Y=T2.X AND T1.Z=T2.Y) OR (T1.X=T2.Z AND T1.Y=T2.Y AND T1.Z=T2.X)
group by T1.num_count,T1.X,T1.Y,T1.Z

