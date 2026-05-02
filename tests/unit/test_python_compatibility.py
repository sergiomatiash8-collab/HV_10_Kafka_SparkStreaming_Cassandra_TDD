def test_python_version_compatible_with_pyspark():
    """
    PySpark 3.5.0 підтримує Python 3.8–3.11.
    Python 3.12+ несумісний — worker крашиться з EOFException / WinError 10038.
    """
    import sys
    major, minor = sys.version_info.major, sys.version_info.minor
    assert major == 3, f"Потрібен Python 3.x, є {major}.{minor}"
    assert minor <= 11, (
        f"Python {major}.{minor} несумісний з PySpark 3.5.0!\n"
        f"Максимальна підтримувана версія: Python 3.11\n"
        f"Рішення: conda create -n spark_env python=3.11 && conda activate spark_env"
    )
    assert minor >= 8, f"Python {major}.{minor} застарів, мінімум 3.8"