import configparser

############# config get
config = configparser.ConfigParser(interpolation=None)
config.read('sync_config.config')
sync_max = int(config['sync config'].get('sync_max'))
sync_count = int(config['sync config'].get('sync_count'))
sync_interval = int(config['sync config'].get('sync_interval'))
iotdb_ip = config['iotdb']['ip']
iotdb_user = config['iotdb']['user']
iotdb_password = config['iotdb']['password']
mysql_ip = config['mysql']['ip']
mysql_user = config['mysql']['user']
mysql_password = config['mysql']['password']
mysql_database = config['mysql']['database']
id_start = int(config['verify config']['id_start'])
id_end = int(config['verify config']['id_end'])
table_name = config['verify config']['table_name']
verify_batchsize = int(config['verify config']['verify_batchsize'])