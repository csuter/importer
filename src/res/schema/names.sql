drop table if exists Names;
create table Names
(
    NameID int unsigned primary key auto_increment,
    FirstName varchar(128) not null,
    LastName varchar(128) not null
) Engine = InnoDB
default charset = utf8
collate = utf8_general_ci;
