-- Q5 Iteration

DROP TABLE IF EXISTS q5_extended_paths;
CREATE TABLE q5_extended_paths(src, dest, length, path)
AS
    WITH X AS(SELECT q1.src, q2.dest, 
                     array_length(array_append(q1.path, q2.dest), 1) - 1 AS length,
                     array_append(q1.path, q2.dest) AS path
              FROM q5_paths AS q1, q5_paths AS q2
              WHERE q1.dest = q2.src
                    AND q2.length = 1
                    AND q1.src != q2.dest
                    AND q1.dest != q2.dest)
    SELECT * FROM X AS x
;

CREATE TABLE q5_new_paths(src, dest, length, path)
AS
    WITH NEW_PATHS AS (SELECT src,dest FROM q5_extended_paths AS qe
                       EXCEPT
                       SELECT src,dest FROM q5_paths AS q)
    SELECT DISTINCT q.src,new.dest,qe.length,qe.path
    FROM q5_paths AS q 
         INNER JOIN q5_extended_paths AS qe ON qe.src = q.src
         INNER JOIN NEW_PATHS AS new ON new.src = qe.src AND new.dest = qe.dest

    -- /////////////////////////////////////////////////////////////////////////
    -- UNION the above with the below to also include paths for which a shorter 
    -- route is found even though it might be longer hope-wise.
    --
    -- This isn't tested but should probably work okay.
    --
    -- WITH X AS((SELECT src,dest FROM q5_extended_paths)
    --           INTERSECT
    --           (SELECT src,dest FROM q5_paths))
    -- SELECT src,dest,length,path 
    -- FROM X as x
    --      INNER JOIN q5_extended_paths AS qe 
    --                 ON qe.src = x.src AND qe.dest = x.dest
    --      INNER JOIN q5_paths AS q 
    --                 ON q.src = qe.src 
    --                 AND q.dest = qe.dest
    --                 AND qe.length < q.length
    ORDER BY q.src
;
CREATE TABLE q5_better_paths(src, dest, length, path)
AS 
    WITH X AS ((SELECT * FROM q5_paths)
               UNION 
               (SELECT *  FROM q5_new_paths))
    SELECT * FROM X
;

DROP TABLE q5_paths;
ALTER TABLE q5_better_paths RENAME TO q5_paths;

DROP TABLE q5_paths_to_update;
ALTER TABLE q5_new_paths RENAME TO q5_paths_to_update;

SELECT COUNT(*) AS path_count,
       CASE WHEN 0 = (SELECT COUNT(*) FROM q5_paths_to_update) 
            THEN 'FINISHED'
            ELSE 'RUN AGAIN' END AS status
  FROM q5_paths;
