-- hint: broadcast

create table edge(F integer, T integer);

SELECT edge.F, edge.T, weight.W
FROM (SELECT F, count(*) as W
      FROM edge
      GROUP BY F) weight, edge
WHERE edge.F = weight.F
