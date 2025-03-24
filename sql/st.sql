create database dev_realtime_v1_huizhong_bai;
use dev_realtime_v1_huizhong_bai;

CREATE TABLE `ods_order_info` (
                                  `id` bigint  COMMENT '编号',
                                  `total_amount` decimal(16,2)  COMMENT '总金额',
                                  `order_status` string  COMMENT '订单状态',
                                  `user_id` bigint  COMMENT '用户id',
                                  `payment_way` string  COMMENT '订单备注',
                                  `out_trade_no` string  COMMENT '订单交易编号（第三方支付用)',
                                  `create_time` string  COMMENT '创建时间',
                                  `operate_time` string  COMMENT '操作时间'
)partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_huizhong_bai.db/ods/ods_order_info/'
TBLPROPERTIES ("parquet.comperssion"="gzip");
load data inpath '/2207A/baihuizhong/order_info/2025-03-23' overwrite into table ods_order_info partition (dt='2025-03-24');

select * from ods_order_info;


CREATE TABLE `ods_order_detail` (
                                    `id` bigint  COMMENT '编号',
                                    `order_id` bigint  COMMENT '订单编号',
                                    `user_id` bigint  COMMENT '用户id',
                                    `sku_id` bigint  COMMENT 'sku_id',
                                    `sku_name` string  COMMENT 'sku名称（冗余)',
                                    `img_url` string  COMMENT '图片名称（冗余)',
                                    `order_price` decimal(10,2)  COMMENT '购买价格(下单时sku价格）',
                                    `sku_num` string  COMMENT '购买个数',
                                    `create_time` string  COMMENT '创建时间'
)partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_huizhong_bai.db/ods/osd_order_detail'
TBLPROPERTIES ("parquet.comperssion"="gzip");
load data inpath '/2207A/baihuizhong/order_detail/2025-03-23' overwrite into table ods_order_detail partition (dt='2025-03-24');
select * from ods_order_detail;

CREATE TABLE `ods_user_info` (
                                 `id` bigint  COMMENT '编号',
                                 `name` string COMMENT '用户姓名',
                                 `birthday` string COMMENT '用户生日',
                                 `gender` string COMMENT '性别 M男,F女',
                                 `email` string COMMENT '邮箱',
                                 `user_level` string COMMENT '用户级别',
                                 `create_time` string COMMENT '创建时间'
)partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_huizhong_bai.db/ods/ods_user_info'
TBLPROPERTIES ("parquet.commperssion"="gzip");
load data inpath '/2207A/baihuizhong/user_info/2025-03-23' overwrite into table ods_user_info partition (dt='2025-03-24');

select * from ods_user_info;

CREATE TABLE `ods_sku_info` (
                                `id` bigint  COMMENT 'skuid(itemID)',
                                `spu_id` bigint  COMMENT 'spuid',
                                `price` decimal(10,0)  COMMENT '价格',
                                `sku_name` string  COMMENT 'sku名称',
                                `sku_desc` string  COMMENT '商品规格描述',
                                `weight` decimal(10,2)  COMMENT '重量',
                                `tm_id` bigint  COMMENT '品牌(冗余)',
                                `category3_id` bigint  COMMENT '三级分类id（冗余)',
                                `create_time` string  COMMENT '创建时间'
) partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_huizhong_bai.db/ods/ods_sku_info'
TBLPROPERTIES ("parquet.commperssion"="gzip");
load data inpath '/2207A/baihuizhong/sku_info/2025-03-23' into table ods_sku_info partition (dt='2025-03-24');

select * from ods_sku_info;

CREATE TABLE `ods_comment_info` (
                                    `id` bigint  COMMENT '编号',
                                    `user_id` bigint  COMMENT '用户名称',
                                    `sku_id` bigint  COMMENT 'skuid',
                                    `spu_id` bigint  COMMENT '商品id',
                                    `order_id` bigint  COMMENT '订单编号',
                                    `appraise` string  COMMENT '评价 1 好评 2 中评 3 差评',
                                    `comment_txt` string  COMMENT '评价内容',
                                    `create_time` string  COMMENT '创建时间',
                                    `operate_time` string  COMMENT '修改时间'
)partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/user/hive/warehouse/dev_realtime_v1_huizhong_bai.db/ods/ods_comment_info'
TBLPROPERTIES ("parquet.commperssion"="gzip");

load data inpath '/2207A/baihuizhong/comment_info/2025-03-23' into table ods_comment_info partition (dt='2025-03-24');

select * from ods_comment_info;
