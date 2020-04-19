#! /bin/bash
###############################
# 脚本功能: 每日增量导入hdfs
# $1: 表名,默认值all
# $2: 日期,默认值执行日期减1天
###############################

source ./../mysql_to_hdfs.sh

table_name=$1
if [[  "x$table_name" = "x" ]]; then
    table_name=all
fi

do_date=$2
if [[  "x$do_date" = "x" ]]; then
    do_date=`date -d '-1 day' +%F`
fi

import_order_status_log(){
  import_data "order_status_log" "$do_date" "select
                                          id,
                                          order_id,
                                          order_status,
                                          operate_time
                                        from order_status_log
                                        where date_format(operate_time,'%Y-%m-%d')='$do_date'"
}

import_activity_order(){
  import_data "activity_order" " $do_date" "select
                                        id,
                                        activity_id,
                                        order_id,
                                        create_time
                                      from activity_order
                                      where date_format(create_time,'%Y-%m-%d')='$do_date'"
}

case $table_name in
  "order_status_log")
     import_order_status_log
;;
  "activity_order")
     import_activity_order
;;
  "all")
     import_order_status_log
     import_activity_order
;;
esac