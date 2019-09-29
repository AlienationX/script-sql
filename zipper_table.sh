#!/bin/bash

# src每天增量分区
# ods层实现拉链数据
# param_format: 2019-01-01

if [[ $# -eq 1 ]]; then
    input_date=`date -d "$1" +%F`
    if [[ $? -ne 0 ]]; then
      echo "Incorrect date format!!!"
      exit 1
    fi
    dt=input_date
else
    dt=$(date -d "-1 day" +"%Y-%m-%d")
fi

flag=$(hive -v -e "select 1 as num from ods_orders limit 1")

# 全量处理sql
hive -v -e "
insert overwrite table dw_order
select b.orderid,
       ...
       b.createtime,
       b.modifiedtime,
       b.status,
       b.start_date,
       lead(b.start_date,1,'9999-12-31') over(partition by b.orderid order by nvl(b.modifiedtime,b.createtime)) as end_date
from (
        select a.orderid,
               ...
               a.createtime,
               a.modifiedtime,
               a.status,
               substr(nvl(a.modifiedtime,a.createtime),1,10) as start_date
        from src_orders a
        where a.dt>='1970-01-01'
      ) b;
"

# 增量处理sql
hive -v -e "
drop table ods_orders_tmp;

create table ods_orders_tmp stored as orc as
select a.orderid,
       a.createtime,
       a.modifiedtime,
       a.status,
       case when b.orderid is null 
from dw_orders a
left join (select * from src_orders where dt='$dt') b on a.orderid=b.orderid
union all
select a.orderid,
       a.createtime,
       a.modifiedtime,
       a.status,
       substr(nvl(a.modifiedtime,a.createtime),1,10) as start_date,
       '9999-12-31' as end_date
from src_orders a
where dt='$dt';

insert overwrite table dw_orders
select * from dw_orders;
"
