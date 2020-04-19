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
drop table if exists dwd.dwd_dim_user_info_his_tmp;
create external table dwd.dwd_dim_user_info_his_tmp(
    `id` string COMMENT '用户id',
    `name` string COMMENT '姓名',
    `birthday` string COMMENT '生日',
    `gender` string COMMENT '性别',
    `email` string COMMENT '邮箱',
    `user_level` string COMMENT '用户等级',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `start_date`  string COMMENT '有效开始日期',
    `end_date`  string COMMENT '有效结束日期'
) COMMENT '用户拉链临时表'
stored as parquet;

insert overwrite table dwd.dwd_dim_user_info_his_tmp
select * from
(
    select --新增和变化的数据
        id,
        name,
        birthday,
        gender,
        email,
        user_level,
        create_time,
        operate_time,
        '$do_date' start_date,
        '9999-99-99' end_date
    from ods.ods_user_info where dt='$do_date'

    union all
    select --对历史数据进行修改后的数据
        uh.id,
        uh.name,
        uh.birthday,
        uh.gender,
        uh.email,
        uh.user_level,
        uh.create_time,
        uh.operate_time,
        uh.start_date,
        if(ui.id is not null  and uh.end_date='9999-99-99', date_add(ui.dt,-1), uh.end_date) end_date
    from dwd.dwd_dim_user_info_his uh left --历史数据
    join
    (
        select --新增和变化的数据
            *
        from ods.ods_user_info
        where dt='$do_date'
    ) ui on uh.id=ui.id
)his
order by his.id, start_date;

insert overwrite table ${APP}.dwd_dim_user_info_his select * from ${APP}.dwd_dim_user_info_his_tmp;
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