#!/bin/bash 
table_name=('sys_veh_produce' 'sys_veh_sale' 'after_sale_repair_record' 'retired_battery_record')
suffix='.txt'
for file in ${table_name[@]}
do
echo ${file}
rm -rf ${file}
done

output_path='/opt/vehicle/mysql-out-data/'
current=`date "+%Y-%m-%d %H:%M:%S"`'--'

host='127.0.0.1'
user='root'
password='qaz123'
database_array=('vehicle01' 'vehicle02' 'vehicle03' 'vehicle04' 'vehicle05' 'vehicle06' 'vehicle07' 'vehicle08' 'vehicle09' 'vehicle10' 'vehicle11' 'vehicle12')
for db in ${database_array[@]}
do
    sql=''
    a='-'
    for table in ${table_name[@]}
    do
    sql=${sql}"select * from ${table} into outfile '${output_path}${current}${db}${a}${table}.txt';"
    done
mysql -h$host -u$user -p$password -e "use ${db};${sql}"
done

for file in ${table_name[@]}
do
mkdir ${file}
mv *${file}${suffix} ${file}
done