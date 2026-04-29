"""
Shared Spark session helper.

On Windows, PySpark needs an explicit JAVA_HOME and a PYSPARK_PYTHON pointing at
the same Python interpreter the driver runs under, otherwise worker processes
fail to spawn with `CreateProcess error=2`. This helper centralises those
settings so every part of the project starts Spark the same way.
"""
import os
import sys
from pyspark.sql import SparkSession

# Resolve the JDK shipped via winget (Microsoft OpenJDK 17). If a user already
# has JAVA_HOME set, we respect it; otherwise we fall back to this default.
DEFAULT_JAVA_HOME = r"C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot"


def _ensure_java_home() -> None:
    java_home = os.environ.get("JAVA_HOME") or DEFAULT_JAVA_HOME
    os.environ["JAVA_HOME"] = java_home
    java_bin = os.path.join(java_home, "bin")
    if java_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")


def _ensure_python_workers() -> None:
    py = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", py)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", py)


def get_spark(app_name: str = "BDA_Assignment", cores: str = "local[*]") -> SparkSession:
    _ensure_java_home()
    _ensure_python_workers()
    spark = (
        SparkSession.builder.appName(app_name)
        .master(cores)
        .config("spark.driver.host", "localhost")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
