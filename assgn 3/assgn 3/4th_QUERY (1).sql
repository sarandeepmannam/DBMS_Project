select ref_to as paper_pid, count(*) as citations_count,(select paper_title from paper where pid=ref_to)as Paper_title,(select main_author from paper where pid=ref_to)as main_author,(select main_author_id from paper where pid=ref_to)as main_author_id,(select year_of_pub from paper where pid=ref_to)as yop,(select publication_venue from paper where pid=ref_to)as Pub_venue,(select abstract from paper where pid=ref_to)as abstract
from temp_citation
group by ref_to
order by citations_count DESC
LIMIT 20