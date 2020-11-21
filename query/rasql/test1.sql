-- hint: broadcast

create table s1(A integer, B integer, C integer);

WITH recursive arc (A, B, C) AS (SELECT * FROM s1)
SELECT a1.C, a2.C, a3.C, a1.A, a2.A, a3.A, a3.B
FROM arc a1, arc a2, arc a3
WHERE a1.B = a2.A and a2.B = a3.A and a3.C < 3
