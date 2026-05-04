import os, sys, shutil
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.resolve()

def pytest_configure(config):
    
    hadoop_home = PROJECT_ROOT / 'hadoop'
    winutils = hadoop_home / 'bin' / 'winutils.exe'
    if winutils.exists():
        os.environ.setdefault('HADOOP_HOME', str(hadoop_home))
        os.environ.setdefault('hadoop.home.dir', str(hadoop_home))
        bin_path = str(hadoop_home / 'bin')
        if bin_path not in os.environ.get('PATH', ''):
            os.environ['PATH'] = bin_path + os.pathsep + os.environ.get('PATH', '')

    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope='session', autouse=True)
def spark_cleanup_manager():
    
    temp_folders = [
        PROJECT_ROOT / 'spark_workspace',
        PROJECT_ROOT / 's_tmp',
    ]
    for folder in temp_folders:
        if folder.exists():
            shutil.rmtree(folder, ignore_errors=True)
    yield
    import time; time.sleep(1)
    for folder in temp_folders:
        if folder.exists():
            shutil.rmtree(folder, ignore_errors=True)

@pytest.fixture(scope='session')
def spark():
    
    from pyspark.sql import SparkSession
    
    
    if os.name == 'nt':
        os.system("taskkill /F /IM java.exe >nul 2>&1")

    
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
    ]

    session = (SparkSession.builder
              .appName("TDD_Integration_Session")
              .config("spark.jars.packages", ",".join(packages))
              .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
              .config("spark.cassandra.connection.host", "localhost")
              .config("spark.driver.host", "127.0.0.1")
              .config("spark.ui.enabled", "false")
              .master("local[1]")
              .getOrCreate())

    yield session
    session.stop()