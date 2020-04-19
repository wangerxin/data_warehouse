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
insert overwrite table dwd.dwd_dim_sku_info partition(dt='$do_date')
select  
    sku.id,
    sku.spu_id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.tm_id,
    ob.tm_name,
    sku.category3_id,
    c2.id category2_id,
    c1.id category1_id,
    c3.name category3_name,
    c2.name category2_name,
    c1.name category1_name,
    spu.spu_name,
    sku.create_time
from
(
    select * from ods.ods_sku_info where dt='$do_date'
)sku
join
(
    select * from ods.ods_base_trademark where dt='$do_date'
)ob on sku.tm_id=ob.tm_id
join
(
    select * from ods.ods_spu_info where dt='$do_date'
)spu on spu.id = sku.spu_id
join 
(
    select * from ods.ods_base_category3 where dt='$do_date'
)c3 on sku.category3_id=c3.id
join 
(
    select * from ods.ods_base_category2 where dt='$do_date'
)c2 on c3.category2_id=c2.id 
join 
(
    select * from ods.ods_base_category1 where dt='$do_date'
)c1 on c2.category1_id=c1.id;
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