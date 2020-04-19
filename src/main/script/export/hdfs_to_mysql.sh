#!/bin/bash
###############################
# 脚本功能: hdfs导出到mysql
# $1: 表名,默认值all_table
# $2: 日期,默认值执行日期减1天
###############################

table_name=$1
if [[  "x$table_name" = "x" ]]; then
    table_name=all_table
fi

do_date=$2
if [[  "x$do_date" = "x" ]]; then
    do_date=`date -d '-1 day' +%F`
fi

hive_db_name=gmall
mysql_db_name=gmall_report

# $1 table_name,$2 do_date,$3 update_key
export_data() {
/opt/module/sqoop/bin/sqoop export \
--connect "jdbc:mysql://hadoop102:3306/gmall_report?useUnicode=true&characterEncoding=utf-8"  \
--username root \
--password 000000 \
--table $1 \
--num-mappers 1 \
--export-dir /origin_data/gmall/db/$1/$2 \
--input-fields-terminated-by "\t" \
--update-mode allowinsert \
--update-key $3 \
--input-null-string '\\N'    \
--input-null-non-string '\\N'
}

