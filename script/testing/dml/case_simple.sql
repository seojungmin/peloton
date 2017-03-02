create table a(id int, value varchar);

insert into a values(1, 'hi');

insert into a values(2, 'ha');

select * from a;

select id, case when id=1 then 'one' when id=2 then 'two' else 'other' end from a;

select id, case when id=1 then 'one' when value='ha' then 'two' else 'other' end from a;

select id, case when id=1 then 'one' when id=2 then 'two' end from a;

select id, case id when 1 then 'one' when 2 then 'two' else 'other' end from a;

select id, case id when 1 then 'one' when 3 then 'two' else 'other' end from a;
