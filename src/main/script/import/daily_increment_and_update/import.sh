#! /bin/bash
###############################
# 脚本功能: 每日将新增和修改的数据导入hdfs
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

import_order_info(){
  import_data "order_info" "$do_date" "select
                                    id,
                                    final_total_amount,
                                    order_status,
                                    user_id,
                                    out_trade_no,
                                    create_time,
                                    operate_time,
                                    province_id,
                                    benefit_reduce_amount,
                                    original_total_amount,
                                    feight_fee
                                from order_info
                                where (date_format(create_time,'%Y-%m-%d')='$do_date'
                                or date_format(operate_time,'%Y-%m-%d')='$do_date')"
}

import_user_info(){
  import_data "user_info" "$do_date" "select
                                    id,
                                    name,
                                    birthday,
                                    gender,
                                    email,
                                    user_level,
                                    create_time,
                                    operate_time
                                  from user_info
                                  where (DATE_FORMAT(create_time,'%Y-%m-%d')='$do_date'
                                  or DATE_FORMAT(operate_time,'%Y-%m-%d')='$do_date')"
}

case $table_name in
  "order_info")
     import_order_info
;;
  "user_info")
     import_user_info
;;
  "all")
     import_order_info
     import_user_info
;;
esac