--Q5 Iteration

DROP TABLE IF EXISTS q5_extnded_paths;
CREATE TABLE q5_extended_paths(src, dest, length, path)
AS
select distinct u.src, e.dest,
(select u1.length + 1 from q5_paths_to_update u1 where u1.src = u.src and u1.dest = u.dest),
(select u2.path || e.dest from q5_paths_to_update u2 where u2.src = u.src and u2.dest = u.dest)
from q5_edges e, q5_paths_to_update u
where u.dest = e.src
and u.dest <> e.dest
;

CREATE TABLE q5_new_paths(src, dest, length, path)
AS
select u.*
from q5_paths_to_update u
where (not exists (select 1 from q5_paths p where p.src = u.src and p.dest = u.dest)
or exists (select 1 from q5_paths p where p.src = u.src and p.dest = u.dest and p.length > u.length))
;

CREATE TABLE q5_better_paths(src, dest, length, path)
AS
select p.src, p.dest, p.length, p.path
from q5_paths p
where (not exists (select 1 from q5_new_paths n where p.src = n.src and p.dest = n.dest)
or exists (select 1 from q5_new_paths n where p.src = n.src and p.dest = n.dest and p.length <= n.length))
union all
select n.src, n.dest, n.length, n.path
from q5_new_paths n
where exists (select 1 from q5_paths p where p.src = n.src and p.dest = n.dest and p.length > n.length) 
;

DROP TABLE q5_paths;
ALTER TABLE q5_better_paths RENAME q5_paths;

DROP TABLE q5_paths_to_update;
ALTER TABLE q5_new_paths RENAME TO q5_paths_to_update;

SELECT COUNT(*) AS path_count,
       CASE WHEN 0 = (SELECT COUNT(*) FROM q5_paths_to_update)
       	    THEN 'FINISHED'
	    ELSE 'RUN AGAIN' END AS status
From q5_paths;
