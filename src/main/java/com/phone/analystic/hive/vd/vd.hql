浏览深度：
kpi_dimension中1代表session角度，2代表用户角度

create table if not exists stats_view_depth(
`platform_dimension_id` int,
`data_dimension_id` int,
`kpi_dimension_id` int,
`pv1` int,
`pv2` int,
`pv3` int,
`pv4` int,
`pv5_10` int,
`pv10_30` int,
`pv30_60` int,
`pv60+` int,
`created` date
)
;

创建临时表：在这里，kpi的维度直接在mysql中写出了，这里并不需要将其写出
pv代表那些维度
ct表示用户id/sessionId的去重个数
create table if not exists stats_view_depth_tmp(
pl string,
dt string,
pv string,
ct string
);

//用户角度
//现在将数据插入到临时表中
set hive.exec.mode.local.auto=true;
set hive.groupby.skewindata=true;
FROM
(
SELECT
p.pl pl,
from_unixtime(cast(p.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
(case
when count(p.p_url)=1 then "pv1"
when count(p.p_url)=2 then "pv2"
when count(p.p_url)=3 then "pv3"
when count(p.p_url)=4 then "pv4"
when count(p.p_url)<10 then "pv5_10"
when count(p.p_url)<30 then "pv10_30"
when count(p.p_url)<60 then "pv30_60"
else "pv60plus"
end) as pv,
p.u_ud as uid
FROM log_phone p
WHERE p.month='09' AND p.day='25' AND p.p_url<>'null' AND p.pl is not null
GROUP BY from_unixtime(cast(p.s_time/1000 as bigint),"yyyy-MM-dd"),p.pl,p.u_ud
) tmp
insert overwrite table stats_view_depth_tmp
SELECT tmp.pl,tmp.dt,tmp.pv,count(distinct tmp.uid)
WHERE tmp.uid is not null
GROUP BY tmp.pl,tmp.dt,tmp.pv
;

//由于case when的存在，所以有几个不同的pv的值，那么就需要把这几个pv的值，合在一起
set hive.exec.mode.local.auto=true;
set hive.groupby.skewindata=true;
WITH tmp AS(
SELECT pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv1' UNION ALL
SELECT pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv2' UNION ALL
SELECT pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv3' UNION ALL
SELECT pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv4' UNION ALL
SELECT pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv5_10' UNION ALL
SELECT pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv10_30' UNION ALL
SELECT pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv30_60' UNION ALL
SELECT pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60plus FROM stats_view_depth_tmp where pv='pv60plus'
)
FROM tmp
INSERT OVERWRITE TABLE stats_view_depth
SELECT phone_platform(pl),phone_date(dt),2,sum(pv1),sum(pv2),sum(pv3),sum(pv4),
sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60plus),dt
GROUP BY pl,dt
;

//会话角度
FROM
(
SELECT
p.pl pl,
from_unixtime(cast(p.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
(case
when count(p.p_url)=1 then "pv1"
when count(p.p_url)=2 then "pv2"
when count(p.p_url)=3 then "pv3"
when count(p.p_url)=4 then "pv4"
when count(p.p_url)<10 then "pv5_10"
when count(p.p_url)<30 then "pv10_30"
when count(p.p_url)<60 then "pv30_60"
else "pv60plus"
end) as pv,
p.u_sd as sessionId
FROM log_phone p
WHERE month='09' AND day='25' AND p.p_url<>'null' AND p.pl is not null
GROUP BY from_unixtime(cast(p.s_time/1000 as bigint),"yyyy-MM-dd"),p.pl,p.u_sd
) tmp
insert overwrite table stats_view_depth_tmp
SELECT tmp.pl,tmp.dt,tmp.pv,count(distinct tmp.sessionId)
WHERE tmp.sessionId is not null
GROUP BY tmp.pl,tmp.dt,tmp.pv
;

WITH tmp AS(
SELECT pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv1' UNION ALL
SELECT pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv2' UNION ALL
SELECT pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv3' UNION ALL
SELECT pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv4' UNION ALL
SELECT pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv5_10' UNION ALL
SELECT pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv10_30' UNION ALL
SELECT pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60plus FROM stats_view_depth_tmp where pv='pv30_60' UNION ALL
SELECT pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60plus FROM stats_view_depth_tmp where pv='pv60plus'
)
FROM tmp
INSERT INTO TABLE stats_view_depth
SELECT phone_platform(pl),phone_date(dt),1,sum(pv1),sum(pv2),sum(pv3),sum(pv4),
sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60plus),dt
GROUP BY pl,dt
;


sqoop export --connect jdbc:mysql://node01:3306/report \
--username root --password root \
--table stats_view_depth --export-dir hdfs://node01:8020/user/hive/warehouse/phone.db/stats_view_depth/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,kpi_dimension_id \
;