分别统计oid的去重数量作为订单数量，使用去重后的订单的支付金额作为订单金额

oid	string	订单id
on	string	订单名称
cua	string	支付金额
cut	string	支付货币类型
pt	string	支付方式

set hive.exec.mode.local.auto=true;
set hive.groupby.skewindata=true;
//创建最终的结果表
create table if not exists stats_order(
pl string,
date string,
cut string,
pt string,
order string,
sorder string,
rorder string,
amount string,
samount string,
ramount string,
tsamount string,
tramount string,
created string
)
partitioned by(month string,day string)
;

//创建临时表用作统计工作
create table if not exists stats_order_amount_tmp(
pl string,
date string,
cut string,
pt string,
en string,
oid string,
cua string
)
stored as orc;

//给临时表加载数据
from log_phone
insert overwrite table stats_order_amount_tmp
select pl,from_unixtime(cast(s_time/1000 as bigint),"yyyy-MM-dd"),cut,pt,en,oid,cua
where oid<>'null'
;

create table if not exists stats_order_amount_tmp1(
pl string,
date string,
cut string,
pt string,
order int,
amount string
)
stored as orc
;

create table if not exists stats_order_amount_tmp2(
pl string,
date string,
cut string,
pt string,
sorder int,
samount string
)
stored as orc
;

create table if not exists stats_order_amount_tmp3(
pl string,
date string,
cut string,
pt string,
rorder int,
ramount string
)
stored as orc
;

from stats_order_amount_tmp
insert overwrite table stats_order_amount_tmp1
select pl,date,cut,pt,count(distinct oid),sum(cua)
where en = 'e_crt' and oid is not null and oid <> 'null'
group by pl,date,cut,pt
insert overwrite table stats_order_amount_tmp2
select pl,date,cut,pt,count(distinct oid),sum(cua)
where en = 'e_cs' and oid is not null and oid <> 'null'
group by pl,date,cut,pt
insert overwrite table stats_order_amount_tmp3
select pl,date,cut,pt,count(distinct oid),sum(cua)
where en = 'e_cr' and oid is not null and oid <> 'null'
group by pl,date,cut,pt
;


//统计表
from(
select tmp1.pl pl,tmp1.date dt,tmp1.cut cut,tmp1.pt pt,tmp1.order order,tmp1.amount amount,
tmp2.sorder sorder,tmp2.samount samount,
(tmp2.samount + if(sor.tsamount is null,0,sor.tsamount)) tsamount,
tmp3.rorder rorder,tmp3.ramount ramount ,
(tmp3.ramount + if(sor.tramount is null,0,sor.tramount)) tramount
from stats_order_amount_tmp1 tmp1
left join stats_order_amount_tmp2 tmp2
on tmp1.pl=tmp2.pl and tmp1.date=tmp2.date and tmp1.cut=tmp2.cut and tmp1.pt=tmp2.pt
left join stats_order_amount_tmp3 tmp3
on tmp1.pl=tmp3.pl and tmp1.date=tmp3.date and tmp1.cut=tmp3.cut and tmp1.pt=tmp3.pt
left join stats_order sor
on tmp1.pl = sor.pl and tmp1.date=sor.date and tmp1.cut=sor.cut and tmp1.pt=sor.pt and sor.month='09' and sor.day='24'
) as tmp
insert into table stats_order partition(month='09',day='25')
select
phone_platform(tmp.pl),
phone_date(tmp.dt),
phone_currency_type(tmp.cut),
phone_payment_type(tmp.pt),
tmp.order,
tmp.sorder,
tmp.rorder,
tmp.amount,
tmp.samount,
tmp.ramount,
tmp.tsamount,
tmp.tramount,
tmp.dt
;

sqoop export --connect jdbc:mysql://node01:3306/report \
--username root --password root \
--table stats_order --export-dir hdfs://node01:8020/user/hive/warehouse/phone.db/stats_order/month=09/day=25/*  \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
;