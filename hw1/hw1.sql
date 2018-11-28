DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv;

-- Question 0
CREATE VIEW q0(era) AS
select max(era)
from pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear) AS
select namefirst, namelast, birthyear 
from master 
where weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear) AS
select namefirst, namelast, birthyear 
from master 
where namefirst like '% %'
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count) AS
select birthyear, avg(height), count(*) 
from master 
group by birthyear 
order by birthyear asc
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count) AS
select birthyear, avg(height) as avgheight, count(*)
from master
group by birthyear
having avg(height) > 70
order by count(*)
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid) AS
select m.namefirst, m.namelast, h.playerid, h.yearid
from master m, halloffame h
where h.playerid = m.playerid
and h.inducted = 'Y'
order by yearid desc
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid) AS
select distinct m.namefirst, m.namelast, m.playerid, c.schoolid, h.yearid
from master m, collegeplaying c, halloffame h, schools s
where m.playerid = c.playerid and m.playerid = h.playerid and s.schoolid = c.schoolid
and h.inducted = 'Y'
and s.schoolstate = 'CA'
order by h.yearid desc, c.schoolid asc, m.playerid asc
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid) AS
select distinct c.playerid,
(select m.namefirst from master m where m.playerid = c.playerid),
(select m.namelast from master m where m.playerid = c.playerid),
c.schoolid
from collegeplaying c, halloffame h
where c.playerid = h.playerid
and h.inducted = 'Y'
order by c.playerid desc, c.schooid asc
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg) AS
select b.playerid, m.namefirst, m.namelast, b.yearid,
cast((b.h + 2*b.h2b + 3*b.h3b +4*hr) as decimal)/b.ab as slg
from batting b, master m
where b.playerid = m.playerid
and b.ab > 50
order by slg desc, b.yearid asc, b.playerid asc
limit 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg) AS
select b.playerid, m.namefirst, m.namelast,
cast(sum(b.h + 2*b.h2b + 3*b.h3b + 4*hr) as decimal)/sum(b.ab) as lslg
from batting b, master m
where b.playerid = m.playerid
group by b.playerid, m.namefirst, m.namelast
having sum(b.ab) > 50
order by lslg desc, b.playerid asc
limit 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg) AS
select m.namefirst, m.namelast, a.lslg
from (select b.playerid,
cast(sum(b.h + 2*b.h2b + 3*b.h3b + 4*hr) as decimal)/sum(b.ab) as lslg,
(select cast(sum(b1.h + 2*b1.h2b + 3*b1.h3b + 4*b1.hr) as decimal)/sum(b1.ab) from batting b1 where b1.playerid = 'mayswi01') as mlslg
from batting b
where b.playerid = m.playerid
group by b.playerid, m.namefirst, m.namelast
having sum(b.ab) > 50) a, master m
where a.playerid = m.playerid
and a.lslg >  a.mlslg
order by a.lslg desc
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev) AS
select a.yearid, a.minslr, a.maxslr, a.avgslr,
(select sqrt(sum(power((s1.salary - avgslr),2))/rowcount) from salaries s1 where s1.yearid = a.yearid) as stddev
from (select s.yearid, min(s.salary) as minslr, max(s.salary) as maxslr, avg(s.salary) as avgslr, count(*) as rowcount from salaries s group by s.yearid) a
order by a.yearid asc 
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count) AS
select t.*,
(select count(*) from salaries where yearid = 2016 and salary between t.low and t.high)
from (select generate_series(0,9) as binid,
generate_series(0,9) * cast((max(salary)-min(salary) as decimal)/10 + min(salary) as low,
generate_series(1,10) * cast((max(salary)-min(salary) as decimal)/10 + min(salary) as high
from salaries
where yearid = 2016) t
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff) AS
select * 
from (select a.yearid,
(a.curr_min - lag(a.curr_min) over (order by a.yearid asc)) as mindiff,
(a.curr_max - lag(a.curr_max) over (order by a.yearid asc)) as maxdiff,
(a.curr_avg - lag(a.curr_avg) over (order by a.yearid asc)) as avgdiff
from (select yearid, min(salary) as curr_min, max(salary) as curr_max, avg(salary) as curr_avg from salaries group by yearid) a) b
where b.yearid > 1985
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid) AS
select s.playerid, m.namefirst, m.namelast, s.salary
from salaries s, master m
where s.playerid = m.playerid
and s.yearid in (2000, 2001)
and s.salary = (select max(s1.salary) from salaries s1 where s1.yearid = s.yearid)
;
