-- hint: shuffle

create table assbl(Part integer, Sub integer);
create table basic(Part integer, Days integer);

WITH recursive actualdays (Part, max() AS Mdays)
AS  (SELECT basic.Part, basic.Days FROM basic)
        UNION
    (SELECT assbl.Part, actualdays.Mdays FROM assbl, actualdays
     WHERE assbl.Sub = actualdays.Part)
SELECT Part, Mdays FROM actualdays
