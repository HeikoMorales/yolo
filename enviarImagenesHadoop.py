from hdfs import InsecureClient

# Conexión al sistema de archivos Hadoop (HDFS)
client = InsecureClient('http://localhost:9870', user='root')

# Ruta local de la carpeta que deseas copiar a Hadoop
carpeta_local = "./runs/pose/images"

# Ruta en HDFS donde deseas copiar la carpeta
carpeta_hdfs = "/test"

# Copia la carpeta y su contenido a HDFS
client.upload(hdfs_path=carpeta_hdfs, local_path=carpeta_local)