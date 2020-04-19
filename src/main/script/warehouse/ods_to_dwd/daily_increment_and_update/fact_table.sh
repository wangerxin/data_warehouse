#! /bin/bash
###############################
# 脚本功能: ods_to_dwd,增量导入
# $1: 表名,默认值all
# $2: 日期,默认值执行日期减1天
# 优化: 超过50张表,可以拆分并行处理
#       支持单表,多表,所有表
###############################

table_name=$1
if [ "x$table_name" = "x" ];then
  table_name="all"
fi

do_date=$2
if [ "x$do_date" = "x" ] ;then
    do_date=`date -d "-1 day" +%F`
fi

APP=gmall
hive=/opt/module/hive/bin/hive

sql="
insert overwrite table dwd.dwd_order_info_his_tmp
select * from
(
select -- 每日变化数据,包括新增和修改
      id,
    total_amount   ,
    order_status ,
    user_id  ,
    payment_way  ,  
    out_trade_no,  
    create_time ,  
    operate_time ,
    '2019-02-14' start_date,
    '9999-99-99' end_date
from dwd.dwd_order_info where dt='2019-02-14'

union all

select -- 这一部分是旧数据,但是需要对部分数据做更改
    oh.id,
    oh.total_amount ,
    oh.order_status ,
    oh.user_id  ,
    oh.payment_way  ,  
    oh.out_trade_no,  
    oh.create_time ,  
    oh.operate_time ,
    oh.start_date,
    if(oi.id is null ,oh.end_date, date_add(oi.dt,-1)) end_date -- 如果没有关联到,取原值,否则取新值
from dwd.dwd_order_info_his oh left join
    (
        select  -- 每日变化数据,包括新增和修改
        *
        from dwd.dwd_order_info  
        where  dt='2019-02-14'
    ) oi on oh.id=oi.id and oh.end_date='9999-99-99' -- 修改的那一条
)his
order by his.id, start_date;
"

case $table_name in
"table_name"){
    $hive -e "$sql1"
    $hive -e "$sql2"
};;
"all"){
    $hive -e "$sql"
};;
esac