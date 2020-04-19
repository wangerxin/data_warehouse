#! /bin/bash
###############################
# 脚本功能: 导出商品主题
# $1: 表名,默认值all_table
# $2: 日期,默认值执行日期减1天
###############################

source ./../hdfs_to_mysql.sh

table_name=$1
if [[  "x$table_name" = "x" ]]; then
    table_name=all_table
fi

do_date=$2
if [[  "x$do_date" = "x" ]]; then
    do_date=`date -d '-1 day' +%F`
fi

case $table_name in
  "ads_uv_count")
     export_data "ads_uv_count" "$do_date" "dt"
;;
  "ads_user_topic")
     export_data "ads_user_topic" "$do_date" "dt"
;;
   "all_table")
     export_data "ads_uv_count" "$do_date" "dt"
     export_data "ads_user_topic" "$do_date" "dt"
;;
esac