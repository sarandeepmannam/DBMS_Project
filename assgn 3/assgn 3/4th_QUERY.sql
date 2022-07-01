select ref_to as pid, count(*) as citations_count
from temp_citation
group by ref_to
ORDER BY citations_count DESC
LIMIT 20