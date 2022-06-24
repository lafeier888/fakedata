#!/bin/bash
source venv/bin/activate
t_start=`date '+%s'`
totalProcess=$1
batchsize=$2
fieldNum=$3

if [ -z "$totalProcess" -o -z "$batchsize" -o -z "$fieldNum" ];then
  echo "totalProcess/batchsize/fieldNum为必传参数"
  exit -1
fi


for i in $(seq 1 $totalProcess)
do
  python main.py $i $batchsize $fieldNum &
done
wait
# 清理
rm data.csv
rm faker.log
# 生成csv header
schema[0]="id"
for i in `seq 1 $fieldNum`
do
 schema[$i]="f$i"
done
(IFS=,; printf %s "${schema[*]}"  > data.csv )
echo>>data.csv
cat *-data*.csv >> data.csv
rm *-data*.csv

schema2[0]="id string"
for i in `seq 1 $fieldNum`
do
 schema2[$i]="f$i string"
done
schemaList=$(IFS=,; printf %s "${schema2[*]}")
totalRows=$((totalProcess*batchsize))
if [ $((totalRows/10000)) -gt 0 ];then
 posix="$((totalRows/10000))w"
 if [ $((totalRows%10000)) -gt 0 ];then
   posix="${posix}$((totalRows%10000))"
 fi
else
 posix=$batchsize
fi
tableName="t_f${fieldNum}_r$posix"
echo "进程数：$totalProcess 数据量$((totalProcess*batchsize))：字段数：$fieldNum" >> faker.log
echo "create table $tableName ($schemaList) row format delimited fields terminated by ',';" >> faker.log
echo "load data local inpath '`pwd`/data.csv' into table $tableName;" >>faker.log
t_end=`date '+%s'`
echo "完成,耗时:$((t_end-t_start))s" >> faker.log
cat faker.log
