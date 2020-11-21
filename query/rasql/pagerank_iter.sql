-- hint: shuffle

create table edge(F integer, T integer);

SELECT m.T, sum(m.W)
FROM (SELECT distinct F
      FROM edge) rank,
     (SELECT edge.F, edge.T, weight.W
      FROM (SELECT F, 1.0/count(*) as W
            FROM edge
            GROUP BY F) weight, edge
      WHERE edge.F = weight.F) m
WHERE rank.F = m.F
GROUP BY m.T
