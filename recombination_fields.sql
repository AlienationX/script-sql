-- 创建复杂数据类型ARRAY、MAP和STRUCT的表，复杂数据类型允许任意层次的嵌套（数据湖）
use tmp;
drop table if exists parquet_nest;
create table if not exists parquet_nest (
    id                  int                                comment 'id',
    name                string                             comment '姓名',
    full_name           map<string, string>                comment '姓和名',
    address             struct<
                            province:string, 
                            city:string, 
                            county:string
                        >                                  comment '地址：省份(province)/城市(city)/区县(county)',
    skills              array<string>                      comment '技能',
    score               map<string, string>                comment '考试成绩',
    record_data_json    string                             comment '记录数据，json格式',
    record_data         struct<
                            visitdate:string,
                            org:string,
                            dept:string,
                            doctor:string,
                            diseases:array<string>,
                            items:array<map<string, string>>,
                            total_fee:decimal(38,4)
                        >                                  comment '记录数据，复合格式',  -- map只能做最深层的类型，因为map的多个key-value的类型必须一致，不能单独制定value的类型。可以使用struct代替
    update_time         timestamp                          comment '更新时间'
)
stored as parquet;

-- 插入数据（insert values方式无法插入，Unable to create temp file for insert values Expression of type TOK_FUNCTION）
insert overwrite table parquet_nest
select
    1, 
    '小明', 
    map('first', 'wang', 'last', 'xiaoming'),                                   -- map(key1, val1, key2, val2, ...)
    -- struct('湖南省', '邵阳市', '新宁县'),                                    -- struct(val1, val2, ...), Struct field names will be col1, col2, ....
    named_struct('province', '湖南省', 'city', '邵阳市', 'county', '新宁县'),   -- named_struct(name1, val1, name2, val2, ...)
    array('python', 'java', 'shell'),                                           -- array(val1, val2, ...)
    str_to_map('chinese:88,math: 95, english:78 ', ',', ':'),                   -- 数字会自动转换成字符串，key-value不能有空格 map<string, string>
    '{"visitdate": "2020-01-13 12:05:08","org": "北京第三医院","dept": "呼吸科","doctor": "张医生","diseases": ["肺结核", "呼吸道感染"],"items":[   {"itemname":"apple", "unit":"个", "price":0.6, "qty":2, "fee":1.2},   {"itemname":"milk", "unit":"瓶", "price":3, "qty":1, "fee":3},   {"itemname":"orange", "unit":"个", "price":0.8, "qty":4, "fee":3.2}],"total_fee":7.4}',
    named_struct(
        'visitdate', '2020-01-13 12:05:08',
        'org', '北京第三医院',
        'dept', '呼吸科',
        'doctor', '张医生',
        'diseases', array('肺结核', '呼吸道感染'),
        'items', array(
           map('itemname','apple', 'unit','个', 'price',0.6, 'qty',2, 'fee',1.2),
           map('itemname','milk', 'unit','瓶', 'price',3, 'qty',1, 'fee',3),
           map('itemname','orange', 'unit','个', 'price',0.8, 'qty',4, 'fee',3.2)
        ),
        'total_fee', cast(7.4 as decimal(38,4))
    ),
    current_timestamp()
;

-- 查询数据
select t.*, 
       t.full_name['first'], 
       t.address.province,
       t.address.city,
       t.address.county,
       t.skills[0],
       t.score['chinese'],
       t.record_data.items[0]["itemname"],
       t.record_data.total_fee,                                -- struct格式自带列名，map格式需要as
       get_json_object(t.record_data_json, '$.visitdate'),
       get_json_object(t.record_data_json, '$.diseases[1]'),
       get_json_object(t.record_data_json, '$.items[0].itemname'),
       get_json_object('[{"name":"王二狗","sex":"男","age":"25"},{"name":"李狗嗨","sex":"男","age":"47"}]', '$.[0]'),  -- json可以使用中括号开始
       get_json_object('[{"name":"王二狗","sex":"男","age":"25"},{"name":"李狗嗨","sex":"男","age":"47"}]', '$.[0].name')
from parquet_nest t
;

--------------------------------------------------------------------------------------------------------
-- 订单 + 明细 的复杂组合
drop table if exists parquet_nest_master_info;
create table if not exists parquet_nest_master_info (
    id                  int                                comment 'id',
    orgname             string                             comment '机构名称',
    deptname            string                             comment '科室',
    doctorname          string                             comment '医生',
    patientname         string                             comment '患者',
    visitdate           string                             comment '就诊日期',
    diseases            array<string>                      comment '疾病诊断',
    details             array<map<string, string>>         comment '处方明细',
    test                array<map<string, string>>         comment '化验',
    pacs                array<map<string, string>>         comment '影像',
    check_up            map<string, string>                comment '体检',
    total_fee           decimal(38,4)                      comment '金额',
    update_time         timestamp                          comment '更新时间'
)
stored as parquet;

insert overwrite table parquet_nest_master_info
select 1 as id,
       '北京第三医院' as orgname,
       '呼吸科' as deptname,
       '张医生' as doctorname,
       '小明' as patientname,
       '2021-12-08 09:12:58' as visitdate,
       array('肺结核', '呼吸道感染') as diseases,
       array(
           map('itemname','apple', 'unit','个', 'price',0.6, 'qty',2, 'fee',1.2),
           map('itemname','milk', 'unit','瓶', 'price',3, 'qty',1, 'fee',3),
           map('itemname','orange', 'unit','个', 'price',0.8, 'qty',4, 'fee',3.2)
       ) as details,
       array(map()) as test,  -- 直接使用null会报错
       array(map()) as pacs,
       str_to_map('身高:88,体重:95,血常规:78,血糖:78hm', ',', ':') as check_up,
       7.4 as total_fee,
       current_timestamp() as update_time
;

select t.*,
       t.diseases[0],
       t.details[0]['itemname'],
       t.pacs[0],
       t.pacs[0]['abc'],
       t.check_up['血糖']
from parquet_nest_master_info t;
;