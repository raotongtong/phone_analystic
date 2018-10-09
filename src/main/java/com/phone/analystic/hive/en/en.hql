create function phone_date as 'com.phone.analystic.hive.DateDimensionUDF' using jar 'hdfs://node01:8020/phone/udfjars/phone_analystic-1.0-SNAPSHOT.jar';

select phone_date("2018-09-25");

create function phone_platform as 'com.phone.analystic.hive.PlatFormDimensionUDF' using jar 'hdfs://node01:8020/phone/udfjars/phone_analystic-1.0-SNAPSHOT.jar';

select phone_platform("website");

create function phone_event as 'com.phone.analystic.hive.EventDimensionUDF' using jar 'hdfs://node01:8020/phone/udfjars/phone_analystic-1.0-SNAPSHOT.jar';

//创建加载数据的临时表
create table if not exists log_phone_tmp(
ver string,
s_time string,
en string,
u_ud string,
u_mid string,
u_sd string,
c_time string,
l string,
b_iev string,
b_rst string,
p_url string,
p_ref string,
tt string,
pl string,
ip string,
oid string,
`on` string,
cua string,
cut string,
pt string,
ca string,
ac string,
kv_ string,
du string,
browserName string,
browserVersion string,
osName string,
osVersion string,
country string,
province string,
city string
)
partitioned by (month string,day string)
;

load data inpath '/ods/09/25' into table log_phone_tmp partition(month=09,day=25);

//创建数据表
create external table if not exists log_phone(
ver string,
s_time string,
en string,
u_ud string,
u_mid string,
u_sd string,
c_time string,
l string,
b_iev string,
b_rst string,
p_url string,
p_ref string,
tt string,
pl string,
ip string,
oid string,
`on` string,
cua string,
cut string,
pt string,
ca string,
ac string,
kv_ string,
du string,
browserName string,
browserVersion string,
osName string,
osVersion string,
country string,
province string,
city string
)
partitioned by (month string,day string)
stored as orc
;

//向hive表中加载数据
from log_phone_tmp
insert into log_phone partition(month=09,day=25)
select
ver,
s_time,
en,
u_ud,
u_mid,
u_sd,
c_time,
l,
b_iev,
b_rst,
p_url,
p_ref,
tt,
pl,
ip,
oid,
'on',
cua,
cut,
pt,
ca,
ac,
kv_,
du,
browserName,
browserVersion,
osName,
osVersion,
country,
province,
city
where month=09 and day=25;

//事件指标
event事件中，计算category和action分组后的记录个数，不涉及到任何的去重操作

查询出指标
select
from_unixtime(cast(p.s_time/1000 as bigint),"yyyy-MM-dd"),
p.pl,
p.ca,
p.ac,
count(1)
from log_phone p
where month=09 and day=25 and en='e_e'
group by from_unixtime(cast(p.s_time/1000 as bigint),"yyyy-MM-dd"),p.pl,p.ca,p.ac
;

创建最终的结果表：
create table if not exists stats_event(
platform_dimension_id int,
date_dimension_id int,
event_dimension_id int,
times int,
created string
)

from(
select
from_unixtime(cast(p.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
p.pl as pl,
p.ca as ca,
p.ac as ac,
count(1) as ct
from log_phone p
where month=09 and day=25 and en='e_e'
group by from_unixtime(cast(p.s_time/1000 as bigint),"yyyy-MM-dd"),p.pl,p.ca,p.ac
) tmp
insert into stats_event
select phone_platform(tmp.pl),phone_date(tmp.dt),phone_event(tmp.ca,tmp.ac),tmp.ct,tmp.dt
;

导入到结果表：
sqoop export --connect jdbc:mysql://node01:3306/report \
--username root --password root -m 1 \
--table stats_event --export-dir hdfs://node01:8020/user/hive/warehouse/phone.db/stats_event/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,event_dimension_id \
;
