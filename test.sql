-- 分区测试
drop table tmp.test_partitions;
create table tmp.test_partitions (
    id int,
    name string
)
partitioned by (year string,month string,dt string);

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table tmp.test_partitions partition (year,month,dt)
select 1 as id,'a' as name,2019 as year,'201901' as month,'2019-01-01' as dt union all
select 2 as id,'a' as name,2019 as year,'201901' as month,'2019-01-02' as dt union all
select 3 as id,'b' as name,2019 as year,'201902' as month,'2019-02-01' as dt union all
select 4 as id,'b' as name,2019 as year,'201902' as month,'2019-02-01' as dt union all
select 5 as id,'b' as name,2019 as year,'201902' as month,'2019-02-02' as dt union all
select 6 as id,'c' as name,2019 as year,'201903' as month,'2019-03-01' as dt union all
select 7 as id,'c' as name,2018 as year,'201812' as month,'2018-12-01' as dt union all
select 8 as id,'d' as name,2019 as year,'201812' as month,'2018-12-01' as dt union all
select 9 as id,'d' as name,2019 as year,'201812' as month,'2018-12-01' as dt;


explain select * from tmp.test_partitions t where t.year='2019';
explain select * from tmp.test_partitions t where t.year='2019' and t.month='201902';
explain select * from tmp.test_partitions t where t.year='2019' and t.dt='2019-02-01';
explain select * from tmp.test_partitions t where t.month='201902' and t.dt='2019-02-01';
explain select * from tmp.test_partitions t where t.dt='2019-02-01';
explain select * from tmp.test_partitions t where t.dt in ('2019-01-01','2019-01-02');
explain select * from tmp.test_partitions t where substr(t.dt,1,4)='2019';
explain select * from tmp.test_partitions t where t.dt>='2019-01' and t.dt<'2019-02';

-- full join 3张表
drop table tmp.test_a;
create table tmp.test_a (
    id int
);
insert overwrite table tmp.test_a select 1 as id union all select 2 as id union all select 3 as id;
drop table tmp.test_b;
create table tmp.test_b (
    id int
);
insert overwrite table tmp.test_b select 1 as id union all select 2 as id union all select 4 as id;
drop table tmp.test_c;
create table tmp.test_c (
    id int
);
insert overwrite table tmp.test_c select 3 as id union all select 4 as id union all select 5 as id;

select coalesce(a.id,b.id,c.id) as id,a.id as a_id,b.id as b_id,c.id as c_id
from tmp.test_a a 
full join tmp.test_b b on a.id=b.id
full join tmp.test_c c on a.id=c.id;

select coalesce(a.id,b.id,c.id) as id,a.id as a_id,b.id as b_id,c.id as c_id
from tmp.test_a a 
full join tmp.test_b b on a.id=b.id
full join tmp.test_c c on b.id=c.id;

select coalesce(a.id,b.id,c.id) as id,a.id as a_id,b.id as b_id,c.id as c_id
from tmp.test_a a 
full join tmp.test_b b on a.id=b.id
full join tmp.test_c c on nvl(a.id,b.id)=c.id;
