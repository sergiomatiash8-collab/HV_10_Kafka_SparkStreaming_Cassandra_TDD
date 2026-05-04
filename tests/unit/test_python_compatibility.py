def test_python_version_compatible_with_pyspark():
    """
    PySpark 3.5.0 supports Python 3.8–3.11.
    Python 3.12+ not compatible — worker crashes with EOFException / WinError 10038.
    """
    import sys
    major, minor = sys.version_info.major, sys.version_info.minor
    assert major == 3, f"Needed Python 3.x, є {major}.{minor}"
    assert minor <= 11, (
        f"Python {major}.{minor} not compatible with PySpark 3.5.0!\n"
        f"Max supported version: Python 3.11\n"
        f"Solution: conda create -n spark_env python=3.11 && conda activate spark_env"
    )
    assert minor >= 8, f"Python {major}.{minor} is too old, min 3.8"