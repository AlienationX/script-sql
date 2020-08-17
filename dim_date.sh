#!/bin/bash

# ==================================================================================
# author:       shuli
# create:       2019-09-18
# table:        dim_date
# description:  时间维度表，生成的日期天数最多为100000天
# param:        开始日期，结束日期
# return:       
# ==================================================================================

start_date="2000-01-01"
end_date="2030-12-31"

script_path=`dirname $(readlink -f $0)`
source ${script_path}/../arguments.sh
source ${script_path}/../config.sh
source ${script_path}/../functions.sh

hive -v -e "
drop table ${HIVE_DB}.dim_date;

create table if not exists ${HIVE_DB}.dim_date (
    id                               int       comment 'ID',
    date_str                         string    comment '日期：2010-01-01',
    date_num                         string    comment '日期：20100101',
    date_sep                         string    comment '日期：2010/01/01',
    date_cn                          string    comment '日期：2010年01月01日',
    date_dt                          timestamp comment '日期：2010-01-01 00:00:00',
    yearmonth_str                    string    comment '年月：2010-01',
    yearmonth                        string    comment '年月：201001',
    yearmonth_cn                     string    comment '年月：2010年01月',
    year                             int       comment '年份：2010',
    year_cn                          string    comment '年份：2010年',
    year_days                        int       comment '本年总天数：365',
    month                            string    comment '月份：01',
    month_int                        int       comment '月份：1',
    month_cn                         string    comment '月份：01月',
    month_en                         string    comment '月份：January',
    month_en_short                   string    comment '月份：Jan',
    month_days                       int       comment '本月总天数：31',
    month_start_date                 string    comment '月份第一天：2010-01-01，同环比使用',
    quarter_int                      string    comment '季度：1',
    quarter_cn                       string    comment '季度：2010年第一季度',
    quarter_days                     int       comment '本季总天数：92',
    quarter_start_date               string    comment '季度第一天：2010-01-01，同环比使用',
    halfyear                         string    comment '半年：1、2分别代表上半年和下半年',
    halfyear_cn                      string    comment '半年：2010年上半年、2010年下半年',
    weekofyear                       int       comment '本周所在年的第几周。注意跨年问题，系统函数，会承接上年的周',
    weekofyear_cn                    string    comment '本周所在年的第几周。注意跨年问题，系统函数，会承接上年的周。2010年第1周',
    weekofmonth_cn                   string    comment '本周所在月的第几周。注意跨月问题，不承接上月的周，不建议使用',
    dayofyear                        int       comment '本天所在年的第几天',
    dayofmonth                       int       comment '本天所在月的第几天',
    dayofweek                        int       comment '本天所在周的第几天',
    week_en                          string    comment '周：Monday,Tuesday...',
    week_en_short                    string    comment '周：MON,TUE...',
    week_cn                          string    comment '周：星期一,星期二...星期日',
    week_start_date                  string    comment '本周开始日期',
    week_end_date                    string    comment '本周结束日期',
    week_concat                      string    comment '周日期连接：2010-01-01~2010-01-07',
    week_concat_full                 string    comment '周日期连接：2010-01-01~2010-01-07(2010年第1周)'
) 
comment 'dim_时间维度表' 
stored as parquet;

set hive.mapred.mode=nonstrict;

insert overwrite table ${HIVE_DB}.dim_date
select row_number() over(order by t.date_str) as id,
       t.date_str                         ,  -- '日期：2010-01-01',
       t.date_num                         ,  -- '日期：20100101',
       t.date_sep                         ,  -- '日期：2010/01/01',
       t.date_cn                          ,  -- '日期：2010年01月01日',
       t.date_dt                          ,  -- '日期：2010-01-01 00:00:00',
       t.yearmonth_str                    ,  -- '年月：2010-01',
       t.yearmonth                        ,  -- '年月：201001',
       t.yearmonth_cn                     ,  -- '年月：2010年01月',
       t.year                             ,  -- '年份：2010',
       t.year_cn                          ,  -- '年份：2010年',
       t.year_days                        ,  -- '本年总天数：365',
       t.month                            ,  -- '月份：01',
       t.month_int                        ,  -- '月份：1',
       t.month_cn                         ,  -- '月份：01月',
       t.month_en                         ,  -- '月份：January',
       t.month_en_short                   ,  -- '月份：Jan',
       t.month_days                       ,  -- '本月总天数：31',
       t.month_start_date                 ,  -- '月份第一天：2010-01-01，同环比使用',
       t.quarter_int                      ,  -- '季度：1',
       t.quarter_cn                       ,  -- '季度：2010年第一季度',
       t.quarter_days                     ,  -- '本季总天数：92',
       t.quarter_start_date               ,  -- '季度第一天：2010-01-01，同环比使用',
       t.halfyear                         ,  -- '半年：1、2分别代表上半年和下半年',
       t.halfyear_cn                      ,  -- '半年：2010年上半年、2010年下半年',
       t.weekofyear                       ,  -- '本周所在年的第几周。注意跨年问题，系统函数，会承接上年的周',
       t.weekofyear_cn                    ,  -- '本周所在年的第几周。注意跨年问题，系统函数，会承接上年的周。2010年第1周',
       t.weekofmonth_cn                   ,  -- '本周所在月的第几周。注意跨月问题，不承接上月的周，不建议使用',
       t.dayofyear                        ,  -- '本天所在年的第几天',
       t.dayofmonth                       ,  -- '本天所在月的第几天',
       t.dayofweek                        ,  -- '本天所在周的第几天',
       t.week_en                          ,  -- '周：Monday,Tuesday...',
       t.week_en_short                    ,  -- '周：MON,TUE...',
       t.week_cn                          ,  -- '周：星期一,星期二...星期日',
       t.week_start_date                  ,  -- '本周开始日期',
       t.week_end_date                    ,  -- '本周结束日期',
       t.week_concat                      ,  -- '周日期连接：2010-01-01~2010-01-07',
       concat(t.week_concat,'(',t.weekofyear_cn,')') as week_concat_full   -- '周日期连接完整名称：2010-01-01~2010-01-07(2010年第1周)'
from   (
        select seq.c_date as date_str,
               regexp_replace(seq.c_date,'-','') as date_num,
               regexp_replace(seq.c_date,'-','/') as date_sep,
               concat(substr(seq.c_date,1,4),'年',substr(seq.c_date,6,2),'月',substr(seq.c_date,9,2),'日') as date_cn,
               cast(seq.c_date as timestamp) as date_dt,
               concat(substr(seq.c_date,1,4),'-',substr(seq.c_date,6,2)) as yearmonth_str,
               concat(substr(seq.c_date,1,4),substr(seq.c_date,6,2)) as yearmonth,
               concat(substr(seq.c_date,1,4),'年',substr(seq.c_date,6,2),'月') as yearmonth_cn,
               year(seq.c_date) as year,
               concat(year(seq.c_date),'年') as year_cn,
               datediff(concat(substr(seq.c_date,1,4),'-12-31'),concat(substr(seq.c_date,1,4),'-01-01')) + 1 as year_days,
               substr(seq.c_date,6,2) as month,
               month(seq.c_date) as month_int,
               concat(substr(seq.c_date,6,2),'月') as month_cn,
               case when substr(seq.c_date,6,2) = '01' then 'January' 
                    when substr(seq.c_date,6,2) = '02' then 'February' 
                    when substr(seq.c_date,6,2) = '03' then 'March' 
                    when substr(seq.c_date,6,2) = '04' then 'April' 
                    when substr(seq.c_date,6,2) = '05' then 'May' 
                    when substr(seq.c_date,6,2) = '06' then 'June' 
                    when substr(seq.c_date,6,2) = '07' then 'July' 
                    when substr(seq.c_date,6,2) = '08' then 'August' 
                    when substr(seq.c_date,6,2) = '09' then 'September' 
                    when substr(seq.c_date,6,2) = '10' then 'October' 
                    when substr(seq.c_date,6,2) = '11' then 'November' 
                    when substr(seq.c_date,6,2) = '12' then 'December' 
                    end as month_en,
               case when substr(seq.c_date,6,2) = '01' then 'Jan' 
                    when substr(seq.c_date,6,2) = '02' then 'Feb' 
                    when substr(seq.c_date,6,2) = '03' then 'Mar' 
                    when substr(seq.c_date,6,2) = '04' then 'Apr' 
                    when substr(seq.c_date,6,2) = '05' then 'May' 
                    when substr(seq.c_date,6,2) = '06' then 'Jun' 
                    when substr(seq.c_date,6,2) = '07' then 'Jul' 
                    when substr(seq.c_date,6,2) = '08' then 'Aug' 
                    when substr(seq.c_date,6,2) = '09' then 'Sep' 
                    when substr(seq.c_date,6,2) = '10' then 'Oct' 
                    when substr(seq.c_date,6,2) = '11' then 'Nov' 
                    when substr(seq.c_date,6,2) = '12' then 'Dec' 
                    end as month_en_short,
               day(last_day(concat(substr(seq.c_date,1,7),'-01'))) as month_days,
               concat(substr(seq.c_date,1,7),'-01') as month_start_date,
               case when substr(seq.c_date,6,2) >= '01' and substr(seq.c_date,6,2) <= '03' then 1 
                    when substr(seq.c_date,6,2) >= '04' and substr(seq.c_date,6,2) <= '06' then 2 
                    when substr(seq.c_date,6,2) >= '07' and substr(seq.c_date,6,2) <= '09' then 3 
                    when substr(seq.c_date,6,2) >= '10' and substr(seq.c_date,6,2) <= '12' then 4 
                    end as quarter_int,
               case when substr(seq.c_date,6,2) >= '01' and substr(seq.c_date,6,2) <= '03' then concat(substr(seq.c_date,1,4),'年第一季度') 
                    when substr(seq.c_date,6,2) >= '04' and substr(seq.c_date,6,2) <= '06' then concat(substr(seq.c_date,1,4),'年第二季度') 
                    when substr(seq.c_date,6,2) >= '07' and substr(seq.c_date,6,2) <= '09' then concat(substr(seq.c_date,1,4),'年第三季度') 
                    when substr(seq.c_date,6,2) >= '10' and substr(seq.c_date,6,2) <= '12' then concat(substr(seq.c_date,1,4),'年第四季度') 
                    end as quarter_cn,
               datediff(
                    case when substr(seq.c_date,6,2) in ('01','02','03') then concat(substr(seq.c_date,1,4),'-03-31')
                         when substr(seq.c_date,6,2) in ('04','05','06') then concat(substr(seq.c_date,1,4),'-06-30')
                         when substr(seq.c_date,6,2) in ('07','08','09') then concat(substr(seq.c_date,1,4),'-09-30')
                         when substr(seq.c_date,6,2) in ('10','11','12') then concat(substr(seq.c_date,1,4),'-12-31') end
                    ,
                    case when substr(seq.c_date,6,2) in ('01','02','03') then concat(substr(seq.c_date,1,4),'-01-01')
                         when substr(seq.c_date,6,2) in ('04','05','06') then concat(substr(seq.c_date,1,4),'-04-01')
                         when substr(seq.c_date,6,2) in ('07','08','09') then concat(substr(seq.c_date,1,4),'-07-01')
                         when substr(seq.c_date,6,2) in ('10','11','12') then concat(substr(seq.c_date,1,4),'-10-01') end
               ) + 1 as quarter_days,
               case when substr(seq.c_date,6,2) in ('01','02','03') then concat(substr(seq.c_date,1,4),'-01-01')
                    when substr(seq.c_date,6,2) in ('04','05','06') then concat(substr(seq.c_date,1,4),'-04-01')
                    when substr(seq.c_date,6,2) in ('07','08','09') then concat(substr(seq.c_date,1,4),'-07-01')
                    when substr(seq.c_date,6,2) in ('10','11','12') then concat(substr(seq.c_date,1,4),'-10-01') 
                    end as quarter_start_date,
               case when substr(seq.c_date,6,2) >= '01' and substr(seq.c_date,6,2) <= '06' then 1 
                    when substr(seq.c_date,6,2) >= '07' and substr(seq.c_date,6,2) <= '12' then 2 
                    end as halfyear,
               case when substr(seq.c_date,6,2) >= '01' and substr(seq.c_date,6,2) <= '06' then concat(substr(seq.c_date,1,4),'年上半年') 
                    when substr(seq.c_date,6,2) >= '07' and substr(seq.c_date,6,2) <= '12' then concat(substr(seq.c_date,1,4),'年下半年') 
                    end as halfyear_cn,
               weekofyear(seq.c_date) as weekofyear,
               concat(case when substr(seq.c_date,5,2)='01' and weekofyear(seq.c_date)>=50 then cast(year(seq.c_date)-1 as string)
                           when substr(seq.c_date,5,2)='12' and weekofyear(seq.c_date)<=10 then cast(year(seq.c_date)+1 as string)
                           else cast(year(seq.c_date) as string) end,
                      '年第',cast(weekofyear(seq.c_date) as string),'周') as weekofyear_cn,
               concat('第',floor(datediff(seq.c_date,date_sub(concat(substr(seq.c_date,1,7),'-01'),pmod(datediff(concat(substr(seq.c_date,1,7),'-01'),'1900-01-01'),7)))/7)+1,'周') as weekofmonth_cn,
               datediff(seq.c_date,concat(substr(seq.c_date,1,4),'-01-01')) + 1 as dayofyear,
               dayofmonth(seq.c_date) as dayofmonth,
               pmod(datediff(seq.c_date,'1900-01-01'),7) + 1 as dayofweek,
               case when pmod(datediff(seq.c_date,'1900-01-01'),7) = 0 then 'Monday'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 1 then 'Tuesday'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 2 then 'Wednesday'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 3 then 'Thursday'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 4 then 'Friday'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 5 then 'Saturday'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 6 then 'Sunday'
                    end as week_en,
               case when pmod(datediff(seq.c_date,'1900-01-01'),7) = 0 then 'MON'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 1 then 'TUE'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 2 then 'WED'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 3 then 'THU'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 4 then 'FRI'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 5 then 'SAT'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 6 then 'SUN'
                    end as week_en_short,
               case when pmod(datediff(seq.c_date,'1900-01-01'),7) = 0 then '星期一'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 1 then '星期二'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 2 then '星期三'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 3 then '星期四'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 4 then '星期五'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 5 then '星期六'
                    when pmod(datediff(seq.c_date,'1900-01-01'),7) = 6 then '星期日' 
                    end as week_cn,
               date_sub(seq.c_date,pmod(datediff(seq.c_date,'1900-01-01'),7)) as week_start_date,
               date_add(seq.c_date,6-pmod(datediff(seq.c_date,'1900-01-01'),7)) as week_end_date,
               concat(date_sub(seq.c_date,pmod(datediff(seq.c_date,'1900-01-01'),7)),'~',date_add(seq.c_date,6-pmod(datediff(seq.c_date,'1900-01-01'),7))) as week_concat
          from (select date_add('$start_date', a.num + b.num + c.num + d.num + e.num) as c_date
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
                         select 0 * 10 as num union all
                         select 1 * 10 as num union all
                         select 2 * 10 as num union all
                         select 3 * 10 as num union all
                         select 4 * 10 as num union all
                         select 5 * 10 as num union all
                         select 6 * 10 as num union all
                         select 7 * 10 as num union all
                         select 8 * 10 as num union all
                         select 9 * 10 as num     
                    ) b, 
                    (
                         select 0 * 100 as num union all
                         select 1 * 100 as num union all
                         select 2 * 100 as num union all
                         select 3 * 100 as num union all
                         select 4 * 100 as num union all
                         select 5 * 100 as num union all
                         select 6 * 100 as num union all
                         select 7 * 100 as num union all
                         select 8 * 100 as num union all
                         select 9 * 100 as num         
                    ) c, 
                    (
                         select 0 * 1000 as num union all
                         select 1 * 1000 as num union all
                         select 2 * 1000 as num union all
                         select 3 * 1000 as num union all
                         select 4 * 1000 as num union all
                         select 5 * 1000 as num union all
                         select 6 * 1000 as num union all
                         select 7 * 1000 as num union all
                         select 8 * 1000 as num union all
                         select 9 * 1000 as num 
                    ) d,
                    (
                         select 0 * 10000 as num union all
                         select 1 * 10000 as num union all
                         select 2 * 10000 as num union all
                         select 3 * 10000 as num union all
                         select 4 * 10000 as num union all
                         select 5 * 10000 as num union all
                         select 6 * 10000 as num union all
                         select 7 * 10000 as num union all
                         select 8 * 10000 as num union all
                         select 9 * 10000 as num 
                    ) e
               ) seq
          where seq.c_date>='$start_date' and seq.c_date<='$end_date'
       ) t
order by date_str
" &&

refreshTable ${HIVE_DB}.dim_date
