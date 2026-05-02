import os
import sys
import shutil
import pytest
from pathlib import Path

# Шлях до кореня проєкту
PROJECT_ROOT = Path(__file__).parent.parent.resolve()

def pytest_configure(config):
    """Глобальне налаштування оточення перед усіма тестами."""
    hadoop_home = PROJECT_ROOT / "hadoop"
    winutils = hadoop_home / "bin" / "winutils.exe"
    
    if not winutils.exists():
        raise FileNotFoundError(f"winutils.exe не знайдено за шляхом: {winutils}")
    
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["hadoop.home.dir"] = str(hadoop_home)
    
    bin_path = str(hadoop_home / "bin")
    if bin_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_path + os.pathsep + os.environ.get("PATH", "")
    
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

@pytest.fixture(scope="session", autouse=True)
def spark_cleanup_manager():
    """
    Автоматична фікстура, яка очищує тимчасові папки Spark
    ДО та ПІСЛЯ завершення всієї сесії тестів.
    """
    # Папки, які ми хочемо тримати чистими
    temp_folders = [
        PROJECT_ROOT / "spark_workspace",
        PROJECT_ROOT / "s_tmp",
        PROJECT_ROOT / "hadoop_env" # Якщо використовувалася раніше
    ]

    # 1. Очистка перед початком
    for folder in temp_folders:
        if folder.exists():
            shutil.rmtree(folder, ignore_errors=True)

    yield # Тут виконуються всі тести проекту

    # 2. Очистка після завершення (даємо 1-2 секунди на закриття JVM)
    import time
    time.sleep(1)
    for folder in temp_folders:
        if folder.exists():
            shutil.rmtree(folder, ignore_errors=True)