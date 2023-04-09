#!/bin/bash

rm -f client*.log
echo "All client log files have been deleted."

export PYTHONPATH=/Users/lxp/code/git_repos/KVPaxos/
python3 ./src/client/client.py --key my_key --value some_value --log_name useless
echo "set to some_value done"

python3 ./src/client/client.py --key my_key --value value11 --log_name client1 &
python3 ./src/client/client.py --key my_key --value value22 --log_name client2 &
python3 ./src/client/client.py --key my_key --value value33 --log_name client3 &
python3 ./src/client/client.py --key my_key --value value44 --log_name client4 &
python3 ./src/client/client.py --key my_key --value value55 --log_name client5 &
python3 ./src/client/client.py --key my_key --value value66 --log_name client6 &
#python3 ./src/client/client.py --key my_key --value value77 --log_name client7 &
#python3 ./src/client/client.py --key my_key --value value88 --log_name client8 &

wait
echo -e "\n\n\n"
# 合并所有的日志文件并按时间排序，然后添加文件名信息
logs=`ls client*.log`
colors=("\033[0;31m" "\033[0;32m" "\033[0;33m" "\033[0;34m" "\033[0;35m" "\033[0;36m" "\033[0;37m" "\033[1;31m" "\033[1;32m" "\033[1;33m" "\033[1;34m" "\033[1;35m" "\033[1;36m" "\033[1;37m" "\033[0;90m" "\033[1;90m")
index=0

# 使用命令替换将结果存入变量log_output中
log_output=$(
for log in $logs
do
    while read line
    do 
        echo -e "${colors[$index]}$line\033[0m"
    done < <(awk -v file=$log '{print "("file")",$0}' $log)

    index=$(((index + 1) % ${#colors[@]}))
done | sort -s -n -k1.1,1.23
)

# 将log_output按时间排序
sorted_output=$(echo "$log_output" | sort -s -k3.1,3)

echo "$sorted_output"