#!/bin/bash
DATAX_HOME=/opt/soft/datax
# 如果传入日期则do_date等于传入的日期，否则等于前一天日期
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi

#处理目标路径，此处的处理逻辑是，如果目标路径不存在，则创建；若存在，则清空，目的是保证同步任务可重复执行
handle_targetdir() {
  hadoop fs -test -e $1
  if [[ $? -eq 1 ]]; then
    echo "路径$1不存在，正在创建......"
    hadoop fs -mkdir -p $1
  else
    echo "路径$1已经存在"
    fs_count=$(hadoop fs -count $1)
    content_size=$(echo $fs_count | awk '{print $3}')
    if [[ $content_size -eq 0 ]]; then
      echo "路径$1为空"
    else
      echo "路径$1不为空，正在清空......"
      hadoop fs -rm -r -f $1/*
    fi
  fi
}

#数据同步
import_data() {
  datax_config=$1
  target_dir=$2
  handle_targetdir $target_dir
  python $DATAX_HOME/bin/datax.py -p" -Dtargetdir=$target_dir" $datax_config
}

case $1 in
"order_info")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.order_info.json /2207A/baihuizhong/order_info/$do_date
  ;;
"base_category1")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.base_category1.json /2207A/baihuizhong/base_category1/$do_date
  ;;
"base_category2")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.base_category2.json /2207A/baihuizhong/base_category2/$do_date
  ;;
"base_category3")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.base_category3.json /2207A/baihuizhong/base_category3/$do_date
  ;;
"order_detail")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.order_detail.json /2207A/baihuizhong/order_detail/$do_date
  ;;
"sku_info")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_baii.sku_info.json /2207A/baihuizhong/sku_info/$do_date
  ;;
"user_info")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.user_info.json /2207A/baihuizhong/user_info/$do_date
  ;;
"payment_info")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.payment_info.json /2207A/baihuizhong/payment_info/$do_date
  ;;
"base_province")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.base_province.json /2207A/baihuizhong/base_province/$do_date
  ;;
"base_region")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.base_region.json /2207A/baihuizhong/base_region/$do_date
  ;;
"base_trademark")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.base_trademark.json /2207A/baihuizhong/base_trademark/$do_date
  ;;
"activity_info")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.activity_info.json /2207A/baihuizhong/activity_info/$do_date
  ;;
"activity_order")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.activity_order.json /2207A/baihuizhong/activity_order/$do_date
  ;;
"cart_info")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.cart_info.json /2207A/baihuizhong/cart_info/$do_date
  ;;
"comment_info")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.comment_info.json/2207A/baihuizhong/comment_info/$do_date
  ;;
"coupon_info")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.coupon_info.json /2207A/baihuizhong/coupon_info/$do_date
  ;;
"coupon_use")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.coupon_use.json /2207A/baihuizhong/coupon_use/$do_date
  ;;
"favor_info")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.favor_info.json /2207A/baihuizhong/favor_info/$do_date
  ;;
"order_refund_info")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.order_refund_info.json /2207A/baihuizhong/order_refund_info/$do_date
  ;;
"order_status_log")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.order_status_log.json /2207A/baihuizhong/order_status_log/$do_date
  ;;
"spu_info")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.spu_info.json /2207A/baihuizhong/spu_info/$do_date
  ;;
"activity_rule")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.activity_rule.json /2207A/baihuizhong/activity_rule/$do_date
  ;;
"base_dic")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.base_dic.json /2207A/baihuizhong/base_dic/$do_date
  ;;
"all")
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.order_info.json /2207A/baihuizhong/order_info/$do_date
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.base_category1.json /2207A/baihuizhong/base_category1/$do_date
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.base_category2.json /2207A/baihuizhong/base_category2/$do_date
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.base_category3.json /2207A/baihuizhong/base_category3/$do_date
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.order_detail.json /2207A/baihuizhong/order_detail/$do_date
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.sku_info.json /2207A/baihuizhong/sku_info/$do_date
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.user_info.json /2207A/baihuizhong/user_info/$do_date
  import_data /opt/soft/datax/job/2207A/dev_realtime_v1_huizhong_bai/import/dev_realtime_v1_huizhong_bai.payment_info.json /2207A/baihuizhong/payment_info/$do_date
  ;;
esac
