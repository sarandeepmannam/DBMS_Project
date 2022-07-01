select aid1,(select First_Name from Author where aid1=AId) as Aid1FirstName,(select Last_Name from Author where aid1=AId) as Aid1LastName, (select Middle_Name from Author where aid1=AId) as Aid1MiddleName, aid2, (select First_Name from Author where aid2=AId) as Aid2FirstName, (select Last_Name from Author where aid2=AId) as Aid2LastName, (select Middle_Name from Author where aid2=AId) as Aid2MiddleName, auth_pair_count
from (select aid1,aid2,count (*) as auth_pair_count
       from (select T1.aid as aid1, T2.aid as aid2,(T1.aid||','||	T2.aid)
             from list_authors as T1, list_authors as T2
             where T1.aid<T2.aid and T1.pid=T2.pid) as tab1
       group by aid1,aid2)  as tab2
where auth_pair_count>1
order by aid1