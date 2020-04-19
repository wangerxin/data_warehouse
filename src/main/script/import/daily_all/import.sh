#! /bin/bash
###############################
# 脚本功能: 每日全量导入hdfs
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

import_base_category1(){
  import_data "base_category1" "$do_date" "select
                                          id,
                                          name
                                        from base_category1 where 1=1"
}

import_base_category2(){
  import_data "base_category2" "$do_date" "select
                                          id,
                                          name,
                                          category1_id
                                        from base_category2 where 1=1"
}

case $table_name in
  "base_category1")
     import_base_category1
;;
  "base_category2")
     import_base_category2
;;
  "all")
     import_base_category1
     import_base_category2
;;
esac

