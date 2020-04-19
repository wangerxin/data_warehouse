#! /bin/bash
###############################
# 脚本功能: ods_to_dwd,全量导入
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
data_base=gmall


sql="
use $data_base;

insert overwrite table dwd.dwd_fact_order_refund_info partition(dt='$do_date')
select
    id,
    user_id,
    order_id,
    sku_id,
    refund_type,
    refund_num,
    refund_amount,
    refund_reason_type,
    create_time
from ods.ods_order_refund_info
where dt='$do_date';

insert overwrite table dwd.dwd_fact_comment_info partition(dt='$do_date')
select
    id,
    user_id,
    sku_id,
    spu_id,
    order_id,
    appraise,
    create_time
from ods.ods_comment_info
where dt='$do_date';
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