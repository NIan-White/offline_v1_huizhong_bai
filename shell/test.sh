#从mysql采集数据
sh quanbu.sh
#把数据上传到hdfs
sh mysql_to_hdfs all
#下载连接器插件
sh install-plugin.sh 2.3.9

