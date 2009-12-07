drop table if exists Names;
create table Names
(
    NameID int unsigned primary key auto_increment,
    FirstNameID int unsigned not null,
    LastName varchar(128) not null
) Engine = InnoDB
default charset = utf8
collate = utf8_general_ci;

drop table if exists FirstNames;
create table FirstNames
(
    FirstNameID int unsigned primary key auto_increment,
    FirstName varchar(128) not null
) Engine = InnoDB
default charset = utf8
collate = utf8_general_ci;

drop table if exists NamesRoles;
create table NamesRoles
(
    NameID int unsigned not null,
    RoleID int unsigned not null,
    primary key(NameID,RoleID)
) Engine=InnoDB;

drop table if exists Roles;
create table Roles
(
    RoleID int unsigned primary key auto_increment,
    RoleName varchar(64)
) Engine = InnoDB
default charset = utf8
collate = utf8_general_ci;

