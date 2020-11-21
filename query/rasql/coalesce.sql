-- hint: broadcast

create table inter(S integer, E integer);

CREATE VIEW lstart(T)
AS  (SELECT a.S FROM inter a, inter b
     WHERE a.S <= b.E
     GROUP BY a.S
     HAVING a.S = min(b.S));

WITH recursive coal (S, max() AS E)
AS  (SELECT lstart.T, inter.E FROM lstart, inter
     WHERE lstart.T = inter.S)
        UNION
    (SELECT coal.S, inter.E FROM coal, inter
     WHERE coal.S <= inter.S AND inter.S <= coal.E)
SELECT S, E FROM coal
