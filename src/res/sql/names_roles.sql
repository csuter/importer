select
    FirstName,
    LastName,
    RoleName
from
    Names N
    join FirstNames FN using (FirstNameID)
    join NamesRoles NR using (NameID)
    join Roles using (RoleID);
