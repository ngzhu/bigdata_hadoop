create database if not exists db_msg;
use db_msg;
--建表
create table db_msg.tb_msg_source(
msg_time string comment "消息发送时间",
sender_name string comment "发送人昵称",
sender_account string comment "发送人账号",
sender_sex string comment "发送人性别",
sender_ip string comment "发送人ip地址",
sender_os string comment "发送人操作系统",
sender_phonetype string comment "发送人手机型号",
sender_network string comment "发送人网络类型",
sender_gps string comment "发送人的GPS定位",
receiver_name string comment "接收人昵称",
receiver_ip string comment "接收人IP",
receiver_account string comment "接收人账号",
receiver_os string comment "接收人操作系统",
receiver_phonetype string comment "接收人手机型号",
receiver_network string comment "接收人网络类型",
receiver_gps string comment "接收人的GPS定位",
receiver_sex string comment "接收人性别",
msg_type string comment "消息类型",
distance string comment "双方距离",
message string comment "消息内容"
);

--加载数据到表中
load data inpath '/chatdemo/data/chat_data-30W.csv' into table tb_msg_source ;

--验证数据加载
select * from tb_msg_source tms limit 100;
--验证表的熟练
select count(*) from tb_msg_source tms ;

-- 去掉 gps 空的，换成月和天，划分经纬度
select * from tb_msg_source tms  where LENGTH(sender_gps) = 0;
--1 过滤gps
insert OVERWRITE table tb_msg_etl
select
 *, 
 date(msg_time) as msg_day,
 hour(msg_time) as msg_hour,
 split(sender_gps,',')[0] as sender_lng,
 split(sender_gps,',')[1] as sender_lat
from db_msg.tb_msg_source WHERE length(sender_gps)>0;

create table db_msg.tb_msg_etl(
msg_time string comment "消息发送时间",
sender_name string comment "发送人昵称",
sender_account string comment "发送人账号",
sender_sex string comment "发送人性别",
sender_ip string comment "发送人ip地址",
sender_os string comment "发送人操作系统",
sender_phonetype string comment "发送人手机型号",
sender_network string comment "发送人网络类型",
sender_gps string comment "发送人的GPS定位",
receiver_name string comment "接收人昵称",
receiver_ip string comment "接收人IP",
receiver_account string comment "接收人账号",
receiver_os string comment "接收人操作系统",
receiver_phonetype string comment "接收人手机型号",
receiver_network string comment "接收人网络类型",
receiver_gps string comment "接收人的GPS定位",
receiver_sex string comment "接收人性别",
msg_type string comment "消息类型",
distance string comment "双方距离",
message string comment "消息内容",
msg_day string comment "消息日",
msg_hour string comment "消息小时",
sender_lng double comment "经度",
sender_lat double comment "纬度"
);

--指标1，统计今日消息总量
--保存结果表
CREATE table if not exists tb_rs_total_msg_cnt
comment "每日消息总量" as
select msg_day, count(*) as total_msg_cnt from tb_msg_etl group by msg_day;

--指标2，统计每小时消息量、发送和接受的用户数
create table tb_rs_hour_msg_cnt comment "每小时消息两趋势" as
select
	msg_hour,
	count(*) as total_msg_cnt,
	count(distinct sender_account) as sender_user_cnt,
	count(DISTINCT receiver_account) as receiver_account_cnt
from db_msg.tb_msg_etl group by msg_hour;

--指标3，统计今日各地区发送信息总量（地区和日期）
create table tb_rs_loc_cnt comment "每日各地区发送消息总量" AS
select
	msg_day,
	sender_lng,
	sender_lat,
	count(*) as total_msg_cnt
from db_msg.tb_msg_etl 
group by msg_day,sender_lng,sender_lat;

--指标4，统计今日发送和接受用户人数
create table tb_rs_user_cnt comment "每日发送和接受消息的人数" as
select
	msg_day,
	count(distinct sender_account) as sender_user_cnt,
	count(distinct receiver_account) as receiver_user_cnt
from db_msg.tb_msg_etl
group by msg_day;

--指标5，统计发送消息条数最多的top10用户
create table tb_rs_s_user_top10 comment "发送消息最多的10个用户" as
select
	sender_name,
	count(*) as sender_msg_cnt
from db_msg.tb_msg_etl
group by sender_name
order by sender_msg_cnt DESC limit 10;

--指标6，统计接受消息最多的top10用户
create table tb_rs_r_user_top10 comment "接受消息最多的10个用户" as
select
	receiver_name,
	count(*) as receiver_msg_cnt
from db_msg.tb_msg_etl
group by receiver_name
order by receiver_msg_cnt DESC limit 10;

--指标7，统计发送人的手机型号分布情况
create table tb_rs_senderphone comment "发送人的手机型号的分布" as
select
	sender_phonetype,
	count(*) as cnt
from db_msg.tb_msg_etl
group by sender_phonetype;

--指标8，统计发送人的手机操作系统分布
create table tb_rs_senderos comment "发送人的os分布" as
select
	sender_os,
	count(*) as cnt
from db_msg.tb_msg_etl
group by sender_os;

desc formatted tb_rs_senderos;