#!/bin/bash
source venv/bin/activate

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
rm data.csv
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
if [ $((batchsize/10000)) -gt 0 ];then
 posix="$((batchsize/10000))w"
else
 posix=$batchsize
fi
echo "create table t_f${fieldNum}_r$posix ($schemaList)"

echo '完成'