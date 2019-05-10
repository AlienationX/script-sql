#!/bin/bash

#  create table dim_date (
#   `date` char(10) DEFAULT NULL COMMENT 'yyyy-mm-dd',
#   `date_dt` datetime DEFAULT NULL COMMENT 'datetime type',
#   `date_str` char(10) DEFAULT NULL COMMENT 'yyyy/mm/dd',
#   `date_cn` char(10) DEFAULT NULL COMMENT 'yyyy年mm月dd日',
#   `year_month` char(6) DEFAULT NULL COMMENT 'yyyymm',
#   `year_month_cn` char(8) DEFAULT NULL COMMENT 'yyyy年mm月',
#   `year` char(4) DEFAULT NULL COMMENT 'yyyy',
#   `month` char(2) DEFAULT NULL COMMENT 'mm',
#   `month_int` int(11) DEFAULT NULL,
#   `month_cn` char(3) DEFAULT NULL COMMENT 'mm月',
#   `month_en` varchar(10) DEFAULT NULL COMMENT 'January',
#   `month_en_short` varchar(10) DEFAULT NULL COMMENT 'Jan',
#   `quarter` int(11) DEFAULT NULL,
#   `half_year` int(11) DEFAULT NULL,
#   `half_year_cn` char(3) DEFAULT NULL COMMENT '上半年 or 下半年',
#   `weekofyear` int(11) DEFAULT NULL,
#   `weekofmonth_cn` char(3) DEFAULT NULL COMMENT '第1周、第2周',
#   `dayofyear` int(11) DEFAULT NULL,
#   `dayofmonth` int(11) DEFAULT NULL,
#   `dayofweek` int(11) DEFAULT NULL,
#   `week_en` varchar(10) DEFAULT NULL COMMENT 'Monday',
#   `week_en_short` varchar(10) DEFAULT NULL COMMENT 'Mon',
#   `week_cn` char(3) DEFAULT NULL COMMENT '星期一',
#   `week_start_date` char(10) DEFAULT NULL COMMENT '周起始日期',
#   `week_end_date` char(10) DEFAULT NULL COMMENT '周结束日期'
# );

start_date=2000-01-01
end_date=2020-12-31

hive -e "
set hive.execution.engine=spark;
set hive.mapred.mode=nonstrict;
create table db.dim_date as
select date,
       to_date(date) as date_dt,
       regexp_replace(date,'-','/') as date_str,
       concat(substr(date,1,4),'年',substr(date,6,2),'月',substr(date,9,2),'日') as date_cn,
       concat(substr(date,1,4),substr(date,6,2)) as year_month,
       concat(substr(date,1,4),'年',substr(date,6,2),'月') as year_month_cn,
       year(date) as year,
       substr(date,6,2) as month,
       month(date) as month_int,
       concat(substr(date,6,2),'月') as month_cn,
       case when substr(date,6,2) = '01' then 'January' 
            when substr(date,6,2) = '02' then 'February' 
            when substr(date,6,2) = '03' then 'March' 
            when substr(date,6,2) = '04' then 'April' 
            when substr(date,6,2) = '05' then 'May' 
            when substr(date,6,2) = '06' then 'June' 
            when substr(date,6,2) = '07' then 'July' 
            when substr(date,6,2) = '08' then 'August' 
            when substr(date,6,2) = '09' then 'September' 
            when substr(date,6,2) = '10' then 'October' 
            when substr(date,6,2) = '11' then 'November' 
            when substr(date,6,2) = '12' then 'December' 
            end as month_en,
       case when substr(date,6,2) = '01' then 'Jan' 
            when substr(date,6,2) = '02' then 'Feb' 
            when substr(date,6,2) = '03' then 'Mar' 
            when substr(date,6,2) = '04' then 'Apr' 
            when substr(date,6,2) = '05' then 'May' 
            when substr(date,6,2) = '06' then 'Jun' 
            when substr(date,6,2) = '07' then 'Jul' 
            when substr(date,6,2) = '08' then 'Aug' 
            when substr(date,6,2) = '09' then 'Sep' 
            when substr(date,6,2) = '10' then 'Oct' 
            when substr(date,6,2) = '11' then 'Nov' 
            when substr(date,6,2) = '12' then 'Dec' 
            end as month_en_short,
       case when substr(date,6,2) >= '01' and substr(date,6,2) <= '03' then 1 
            when substr(date,6,2) >= '04' and substr(date,6,2) <= '06' then 2 
            when substr(date,6,2) >= '07' and substr(date,6,2) <= '09' then 3 
            when substr(date,6,2) >= '10' and substr(date,6,2) <= '12' then 4 
            end as quarter,
       case when substr(date,6,2) >= '01' and substr(date,6,2) <= '06' then 1 
            when substr(date,6,2) >= '07' and substr(date,6,2) <= '12' then 2 
            end as half_year,
       case when substr(date,6,2) >= '01' and substr(date,6,2) <= '06' then '上半年' 
            when substr(date,6,2) >= '07' and substr(date,6,2) <= '12' then '下半年' 
            end as half_year_cn,
       weekofyear(date) as weekofyear,
       concat('第',floor(datediff(date,date_sub(concat(substr(date,1,7),'-01'),pmod(datediff(concat(substr(date,1,7),'-01'),'1900-01-01'),7)))/7)+1,'周') as weekofmonth_cn,
       datediff(date,concat(substr(date,1,4),'-01-01')) + 1 as dayofyear,
       dayofmonth(date) as dayofmonth,
       pmod(datediff(date,'1900-01-01'),7) + 1 as dayofweek,
       case when pmod(datediff(date,'1900-01-01'),7) = 0 then 'Monday'
            when pmod(datediff(date,'1900-01-01'),7) = 1 then 'Tuesday'
            when pmod(datediff(date,'1900-01-01'),7) = 2 then 'Wednesday'
            when pmod(datediff(date,'1900-01-01'),7) = 3 then 'Thursday'
            when pmod(datediff(date,'1900-01-01'),7) = 4 then 'Friday'
            when pmod(datediff(date,'1900-01-01'),7) = 5 then 'Saturday'
            when pmod(datediff(date,'1900-01-01'),7) = 6 then 'Sunday'
            end as week_en,
       case when pmod(datediff(date,'1900-01-01'),7) = 0 then 'MON'
            when pmod(datediff(date,'1900-01-01'),7) = 1 then 'TUE'
            when pmod(datediff(date,'1900-01-01'),7) = 2 then 'WED'
            when pmod(datediff(date,'1900-01-01'),7) = 3 then 'THU'
            when pmod(datediff(date,'1900-01-01'),7) = 4 then 'FRI'
            when pmod(datediff(date,'1900-01-01'),7) = 5 then 'SAT'
            when pmod(datediff(date,'1900-01-01'),7) = 6 then 'SUN'
            end as week_en_short,
       case when pmod(datediff(date,'1900-01-01'),7) = 0 then '星期一'
            when pmod(datediff(date,'1900-01-01'),7) = 1 then '星期二'
            when pmod(datediff(date,'1900-01-01'),7) = 2 then '星期三'
            when pmod(datediff(date,'1900-01-01'),7) = 3 then '星期四'
            when pmod(datediff(date,'1900-01-01'),7) = 4 then '星期五'
            when pmod(datediff(date,'1900-01-01'),7) = 5 then '星期六'
            when pmod(datediff(date,'1900-01-01'),7) = 6 then '星期日' 
            end as week_cn,
       date_sub(date,pmod(datediff(date,'1900-01-01'),7)) as week_start_date,
       date_add(date,6-pmod(datediff(date,'1900-01-01'),7)) as week_end_date
  from (select date_add('$start_date',a.num+b.num+c.num+d.num) as date
        from (
                select 0 as num union all
                select 1 as num union all
                select 2 as num union all
                select 3 as num union all
                select 4 as num union all
                select 5 as num union all
                select 6 as num union all
                select 7 as num union all
                select 8 as num union all
                select 9 as num 
            ) a, 
            (
                select 0*10 as num union all
                select 1*10 as num union all
                select 2*10 as num union all
                select 3*10 as num union all
                select 4*10 as num union all
                select 5*10 as num union all
                select 6*10 as num union all
                select 7*10 as num union all
                select 8*10 as num union all
                select 9*10 as num     
            ) b, 
            (
                select 0*100 as num union all
                select 1*100 as num union all
                select 2*100 as num union all
                select 3*100 as num union all
                select 4*100 as num union all
                select 5*100 as num union all
                select 6*100 as num union all
                select 7*100 as num union all
                select 8*100 as num union all
                select 9*100 as num         
            ) c, 
            (
                select 0*1000 as num union all
                select 1*1000 as num union all
                select 2*1000 as num union all
                select 3*1000 as num union all
                select 4*1000 as num union all
                select 5*1000 as num union all
                select 6*1000 as num union all
                select 7*1000 as num union all
                select 8*1000 as num union all
                select 9*1000 as num 
            ) d
       ) t
 where date>='$start_date' and date<='$end_date'
 order by date
"
