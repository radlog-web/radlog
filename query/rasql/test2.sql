-- hint: broadcast

create table s1(A integer, B integer, C integer);

WITH recursive arc (A, B, C) AS (SELECT * FROM s1)
SELECT a1.A, count(a1.A)
FROM arc a1, arc a2
WHERE a1.B = a2.A
GROUP BY a1.A
