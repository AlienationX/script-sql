---------------------------------------------------------------------------------------------- 分区测试
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

---------------------------------------------------------------------------------------------- full join 3张表
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

---------------------------------------------------------------------------------------------- 
-- 多张表的指标合并，缺失日期数据补全问题，只能一层一层full join，或者使用日期维度表拼接？？？
-- 错误演示
drop table tmp.test_filldate_a;
create table tmp.test_filldate_a (
    dt string,
    orgid string,
    num int
);
insert overwrite table tmp.test_filldate_a
select '2019-01-01' as dt,'A' as orgid,6 as num union all
select '2019-01-01' as dt,'B' as orgid,8 as num union all
select '2019-01-04' as dt,'A' as orgid,9 as num union all
select '2019-01-05' as dt,'A' as orgid,3 as num;

drop table tmp.test_filldate_b;
create table tmp.test_filldate_b (
    dt string,
    orgid string,
    amount double
);
insert overwrite table tmp.test_filldate_b
select '2019-01-01' as dt,'A' as orgid,60 as amount union all
select '2019-01-01' as dt,'B' as orgid,80 as amount union all
select '2019-01-02' as dt,'A' as orgid,90 as amount union all
select '2019-01-03' as dt,'A' as orgid,30 as amount;


select d.date_str,
       nvl(a.dt,b.dt) as dt,
       nvl(a.orgid,b.orgid) as orgid,
       a.num,
       b.amount
from medical.dim_date d
left join tmp.test_filldate_a a on d.date_str=a.dt
left join tmp.test_filldate_b b on d.date_str=b.dt
where a.dt is not null or b.dt is not null;
