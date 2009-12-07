select * from Names;
select * from FirstNames;
select * from LastNames;

select
    FirstName,
    LastName
from
    Names N
    join FirstNames FN using (FirstNameID);
