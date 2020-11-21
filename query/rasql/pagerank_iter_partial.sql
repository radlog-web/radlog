-- hint: shuffle

create table matrix(F integer, T integer, W double);

SELECT m.T, sum(1.0/W)
FROM (SELECT distinct F
      FROM matrix) rank, matrix m
WHERE rank.F = m.F
GROUP BY m.T
