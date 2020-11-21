-- hint: broadcast

create table arc(A integer, B integer);

WITH recursive tc (A, B)
AS  (SELECT A, B FROM arc)
        UNION
    (SELECT tc.A, arc.B FROM tc, arc WHERE tc.B = arc.A)
SELECT A, B FROM tc
