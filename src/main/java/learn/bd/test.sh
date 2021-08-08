#!/bin/bash

# 补全历史数据，(1)传参：开始日期，结束日期 ; (2)倒序

      # 变量定义
      start_dt=$1
      end_dt=$2
      start_dt=`date -d "$start_dt" +%s` # 转成秒
      end_dt=`date -d "$end_dt" +%s` # 转成秒
      st=$(date +%s) # 计时器，当前时刻
      i=$start_dt
      tmp1=''
      tmp2=''
      druid='druid'

      # 遍历
      for (( i=$end_dt; i>=$start_dt; i-=864000 ));
        do
        {
         # 时间转换，每隔9天执行，并发
         tmp2=`date -d "@$i" +%Y-%m-%d` # 将秒转成日期
         let i2=$i-86400*9
         tmp1=`date -d "@$i2" +%Y-%m-%d` # 将秒转成日期

         echo "/opt/xc/scripts/tmp_load_hive2druid_dau_mau.sh $druid  $tmp1  $tmp2" # import
         #/opt/xc/scripts/tmp_load_hive2druid_dau_mau.sh $druid  $tmp1  $tmp2
       }
     done
     echo "/opt/xc/scripts/tmp_load_hive2druid_dau_mau.sh $druid 2021-01-01 "
     #/opt/xc/scripts/tmp_load_hive2druid_dau_mau.sh $druid 2021-01-01
