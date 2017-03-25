drop table if exists jsonfiles;
create table jsonfiles (
    filetitle text primary key,
    description text not null,
    uploadedtime timestamp not null,
    filename text not null
);