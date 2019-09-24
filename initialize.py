import configparser as cfg
import logging as log
# import enlighten
import boto3
import sys
import os

# Configuraciones basicas
log.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=log.INFO)
log.getLogger("botocore.vendored.requests.packages.urllib3").setLevel(log.WARNING)
config = cfg.ConfigParser()
config.read('configuracion.ini')
ENVIRONMENT = sys.argv[1]
FECHA = sys.argv[2]
S3_ACCESS_KEY = config['Credenciales']['S3_ACCESS_KEY']
S3_SECRET_KEY = config['Credenciales']['S3_SECRET_KEY']
# manager = enlighten.get_manager()
CARGA_FULL = False
if len(sys.argv) > 3:
	if sys.argv[3] == 'FULL':
		CARGA_FULL = True
		log.info("Se hara una carga full ;")
else:
	log.info("Se hara una carga regular ;")
s3 = boto3.client('s3', aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)
