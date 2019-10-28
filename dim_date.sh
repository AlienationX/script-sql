#!/bin/bash

# ==================================================================================
# author:       shuli
# create:       2019-09-18
# table:        dim_date
# description:  时间维度表，生成的日期天数最多为100000天
# param:        开始日期，结束日期
# return:       
# ==================================================================================

start_date=2000-01-01
end_date=2030-12-31

hive -v -e "
create table if not exists medical.dim_date (
    id                               int       comment 'ID',
    date_str                         string    comment '日期：2010-01-01',
    date_num                         string    comment '日期：20100101',
    date_sep                         string    comment '日期：2010/01/01',
    date_cn                          string    comment '日期：2010年01月01日',
    date_dt                          timestamp comment '日期：2010-01-01 00:00:00',
    yearmonth                        string    comment '年月：201001',
    yearmonth_cn                     string    comment '年月：2010年01月',
    year                             int       comment '年份：2010',
    year_cn                          string    comment '年份：2010年',
    month                            string    comment '月份：01',
    month_int                        int       comment '月份：1',
    month_cn                         string    comment '月份：01月',
    month_en                         string    comment '月份：January',
    month_en_short                   string    comment '月份：Jan',
    quarter_int                      string    comment '季度：1',
    quarter_cn                       string    comment '季度：2010年第一季度',
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
    week_end_date                    string    comment '本周结束日期'
) 
comment 'dim_时间维度表' 
stored as parquet;

set hive.mapred.mode=nonstrict;

insert overwrite table medical.dim_date
select row_number() over(order by date) as id,
       date as date_str,
       regexp_replace(date,'-','') as date_num,
       regexp_replace(date,'-','/') as date_sep,
       concat(substr(date,1,4),'年',substr(date,6,2),'月',substr(date,9,2),'日') as date_cn,
       cast(date as timestamp) as date_dt,
       concat(substr(date,1,4),substr(date,6,2)) as yearmonth,
       concat(substr(date,1,4),'年',substr(date,6,2),'月') as yearmonth_cn,
       year(date) as year,
       concat(year(date),'年') as year_cn,
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
            end as quarter_int,
       case when substr(date,6,2) >= '01' and substr(date,6,2) <= '03' then concat(substr(date,1,4),'年第一季度') 
            when substr(date,6,2) >= '04' and substr(date,6,2) <= '06' then concat(substr(date,1,4),'年第二季度') 
            when substr(date,6,2) >= '07' and substr(date,6,2) <= '09' then concat(substr(date,1,4),'年第三季度') 
            when substr(date,6,2) >= '10' and substr(date,6,2) <= '12' then concat(substr(date,1,4),'年第四季度') 
            end as quarter_cn,
       case when substr(date,6,2) >= '01' and substr(date,6,2) <= '06' then 1 
            when substr(date,6,2) >= '07' and substr(date,6,2) <= '12' then 2 
            end as halfyear,
       case when substr(date,6,2) >= '01' and substr(date,6,2) <= '06' then concat(substr(date,1,4),'年上半年') 
            when substr(date,6,2) >= '07' and substr(date,6,2) <= '12' then concat(substr(date,1,4),'年下半年') 
            end as halfyear_cn,
       weekofyear(date) as weekofyear,
       concat(case when substr(date,5,2)='01' and weekofyear(date)>=50 then cast(year(date)-1 as string)
                   when substr(date,5,2)='12' and weekofyear(date)<=10 then cast(year(date)+1 as string)
                   else cast(year(date) as string) end,
             '年第',cast(weekofyear(date) as string),'周') weekofyear_cn,
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
  from (select date_add('$start_date',a.num+b.num+c.num+d.num+e.num) as date
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
            ) d,
            (
                select 0*10000 as num union all
                select 1*10000 as num union all
                select 2*10000 as num union all
                select 3*10000 as num union all
                select 4*10000 as num union all
                select 5*10000 as num union all
                select 6*10000 as num union all
                select 7*10000 as num union all
                select 8*10000 as num union all
                select 9*10000 as num 
            ) e
       ) t
where date>='$start_date' and date<='$end_date'
order by date_str
"
