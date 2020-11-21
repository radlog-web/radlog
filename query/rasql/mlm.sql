-- hint: shuffle

create table sales(M integer, P double);
create table sponsor(M1 integer, M2 integer);

WITH recursive bonus(M, sum() as B)
AS  (SELECT M, P * 0.1 FROM sales)
         UNION
    (SELECT sponsor.M1, bonus.B * 0.5
     FROM bonus, sponsor
     WHERE bonus.M = sponsor.M2)
SELECT M, B FROM bonus
