# tests/conftest.py  ← створити цей файл у корені папки tests/
import os
import sys
from pathlib import Path

def pytest_configure(config):
    """
    Встановлюємо HADOOP_HOME до старту будь-якого тесту.
    Це єдине місце — не треба дублювати в кожному тест-файлі.
    
    Spark на Windows вимагає winutils.exe в %HADOOP_HOME%/bin/.
    Папка hadoop/ вже є в проєкті — просто вказуємо на неї.
    """
    # Шлях до кореня проєкту (піднімаємось з tests/ на один рівень вгору)
    project_root = Path(__file__).parent.parent.resolve()
    hadoop_home = project_root / "hadoop"
    
    # Перевіряємо що winutils.exe реально існує — інакше помилка буде та сама
    winutils = hadoop_home / "bin" / "winutils.exe"
    if not winutils.exists():
        raise FileNotFoundError(
            f"winutils.exe не знайдено: {winutils}\n"
            f"Скачай з: https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin"
        )
    
    # Встановлюємо обидві змінні — Spark перевіряє обидві
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["hadoop.home.dir"] = str(hadoop_home)
    
    # Додаємо bin до PATH щоб Java міг знайти winutils.exe напряму
    bin_path = str(hadoop_home / "bin")
    if bin_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_path + os.pathsep + os.environ.get("PATH", "")
    
    # Додаємо корінь проєкту до sys.path щоб працювали імпорти src.*
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))