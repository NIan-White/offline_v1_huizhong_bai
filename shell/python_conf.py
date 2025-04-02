# -*- coding: utf-8 -*-
import os

mysql_config = {
    "url": "jdbc:mysql://cdh03:3306/dev_realtime_v3_huizhong_bai",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "root",
    "password": "root"
}

hive_config = {
    "metastore_uri": "thrift://cdh01:9083",
    "database": "dev_realtime_v3_huizhong_bai",
    "hive_hadoop_conf_path": "/etc/hadoop/conf",
    "save_mode": "overwrite"
}

target_dir = "/opt/soft/seatunnel/seatunnel/job/"


tables = [
    "dim_category","dim_product","dim_user","dwd_order","dwd_payment","dwd_refund","dwd_user_behavior_log"
]  # 要传输的表名列表

for table in tables:
    config_content = """
env {{
    parallelism = 2
    job.mode = "BATCH"
}}

source {{
    Jdbc {{
        url = "{mysql_url}"
        driver = "{mysql_driver}"
        user = "{mysql_user}"
        password = "{mysql_password}"
        table_path = "{hive_database}.{table}"
        query = "SELECT * FROM {hive_database}.{table}"
    }}
}}

transform {{
    # 可按需添加数据转换操作
}}

sink {{
    Hive {{
        metastore_uri = "{hive_metastore_uri}"
        table_name = "{hive_database}.{table}"
        hive.hadoop.conf-path = "{hive_hadoop_conf_path}"
        save_mode = "{hive_save_mode}"
    }}
}}
""".format(
        mysql_url=mysql_config['url'],
        mysql_driver=mysql_config['driver'],
        mysql_user=mysql_config['user'],
        mysql_password=mysql_config['password'],
        hive_database=hive_config['database'],
        table=table,
        hive_metastore_uri=hive_config['metastore_uri'],
        hive_hadoop_conf_path=hive_config['hive_hadoop_conf_path'],
        hive_save_mode=hive_config['save_mode']
    )
    config_file_name = os.path.join(target_dir, "seatunnel_{}.conf".format(table))
    try:
        with open(config_file_name, "w") as f:
            f.write(config_content)
        print("成功生成配置文件: {}".format(config_file_name))
        # 可选择直接执行生成的脚本
        os.system("/opt/soft/seatunnel/bin/seatunnel.sh --config {} -m local".format(config_file_name))
    except Exception as e:
        print("生成配置文件 {} 时出错: {}".format(config_file_name, e))
    