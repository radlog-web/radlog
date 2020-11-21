-- hint: broadcast

create table seg(S integer, E integer);
create table lstart(T integer);

WITH recursive coal (S, max() AS E)
AS  (SELECT lstart.T, seg.E
     FROM lstart, seg
     WHERE lstart.T = seg.S)
        UNION
    (SELECT coal.S, seg.E
     FROM coal, seg
     WHERE coal.S <= seg.S AND seg.S <= coal.E)
SELECT S, E FROM coal
