#!/bin/bash 
table_name=('sys_veh' 'sys_battery_pack_entity')
suffix='.txt'
for file in ${table_name[@]}
do
echo ${file}
rm -f ${file}${suffix}
done

output_path='/opt/vehicle/mysql-out-data/'
current=`date "+%Y-%m-%d %H:%M:%S"`'--'
sql=''
for table in ${table_name[@]}
do
sql=${sql}"select * from ${table} into outfile '${output_path}${current}${table}.txt';"
done

host='127.0.0.1'
user='root'
password='qaz123'
database_array=('batteryvehicle2.0')
for db in ${database_array[@]}
do
mysql -h$host -u$user -p$password -e "use ${db};${sql}"
done
