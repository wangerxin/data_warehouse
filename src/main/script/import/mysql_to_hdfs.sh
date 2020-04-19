#! /bin/bash

sqoop=/opt/module/sqoop/bin/sqoop

# $1 table_name,$2 partition,$3 sql
import_data(){
$sqoop import \
--connect jdbc:mysql://hadoop102:3306/gmall \
--username root \
--password 000000 \
--target-dir /origin_data/gmall/db/$1/$2 \
--delete-target-dir \
--query "$3 and  \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--null-string '\\N' \
--null-non-string '\\N'
}
