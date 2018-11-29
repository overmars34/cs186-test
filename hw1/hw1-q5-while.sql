--Q5 Iteration

DROP TABLE IF EXISTS q5_extended_paths;
CREATE TABLE q5_extended_paths(src, dest, length, path)
AS
select distinct u.src, e.dest, u.length + 1, u.path || e.dest
from q5_edges e, q5_paths_to_update u
where u.dest = e.src
and u.src != e.dest
;

CREATE TABLE q5_new_paths(src, dest, length, path)
AS
select * from q5_extended_paths e
where not exists (select 1 from q5_paths p where p.src = e.src and p.dest = e.dest)
union
select * from q5_extended_paths e
where exists (select 1 from q5_paths p where p.src = e.src and p.dest = e.dest and p.length > e.length)
;

CREATE TABLE q5_better_paths(src, dest, length, path)
AS
select * from q5_new_paths
union
select * from q5_paths 
;

DROP TABLE q5_paths;
ALTER TABLE q5_better_paths RENAME TO q5_paths;

DROP TABLE q5_paths_to_update;
ALTER TABLE q5_new_paths RENAME TO q5_paths_to_update;

SELECT COUNT(*) AS path_count,
       CASE WHEN 0 = (SELECT COUNT(*) FROM q5_paths_to_update)
       	    THEN 'FINISHED'
	    ELSE 'RUN AGAIN' END AS status
From q5_paths;
