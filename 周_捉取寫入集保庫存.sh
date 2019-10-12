#!/bin/bash

current_date_time="`date "+%Y-%m-%d %H:%M:%S"`";

echo $current_date_time;

cd /home/wenwei/下載/

/home/wenwei/miniconda3/envs/py36/bin/python /home/wenwei/下載/捉取寫入集保庫存.py \
"20190927" "20190920" "20190912" "20190906" "20190830" "20191005"

echo "執行捉取寫入集保庫存結束"

exit 0
