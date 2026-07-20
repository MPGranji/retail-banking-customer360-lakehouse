# =============================================================================
# Jupyter Image — JupyterLab 4.3.8 + PySpark kết nối Spark cluster
# Base: lakehouse-spark:3.5.0 (có sẵn Iceberg JARs + JDBC drivers)
# =============================================================================
FROM lakehouse-spark:3.5.0

USER root

ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
ENV PATH="/usr/local/bin:${SPARK_HOME}/bin:${PATH}"
# Đảm bảo driver và executor đều dùng cùng python3
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Cài pip + JupyterLab và các thư viện data engineering
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    jupyterlab==4.3.8 \
    ipykernel \
    ipython \
    pandas \
    pyarrow \
    boto3 \
    psycopg2-binary \
    oracledb \
    tabulate

# ---------------------------------------------------------------------------
# IPython startup script:
# - Được chạy tự động khi bất kỳ kernel Python nào khởi động
# - Script chỉ khởi tạo SparkSession khi env var INIT_PYSPARK=true
#   (giá trị này được set trong kernel.json của PySpark kernel)
# ---------------------------------------------------------------------------
RUN mkdir -p /home/spark/.ipython/profile_default/startup
COPY startup/00_init_spark.py /home/spark/.ipython/profile_default/startup/00_init_spark.py

# ---------------------------------------------------------------------------
# Kernel spec "PySpark (Lakehouse)" — kernel.json không còn dùng
# IPYTHONSTARTUP (không tin cậy), thay bằng startup directory + env var
# ---------------------------------------------------------------------------
COPY kernels/pyspark-lakehouse /usr/local/share/jupyter/kernels/pyspark-lakehouse

# ---------------------------------------------------------------------------
# Jupyter server config: không token/password cho dev local
# ---------------------------------------------------------------------------
RUN mkdir -p /home/spark/.jupyter && \
    printf '%s\n' \
        "c.ServerApp.token = ''" \
        "c.ServerApp.password = ''" \
        "c.ServerApp.open_browser = False" \
        "c.ServerApp.ip = '0.0.0.0'" \
        "c.ServerApp.port = 8888" \
        "c.ServerApp.root_dir = '/opt/notebooks'" \
    > /home/spark/.jupyter/jupyter_server_config.py

# Tạo thư mục notebooks + fix ownership
RUN mkdir -p /opt/notebooks && \
    chown -R spark:spark /opt/notebooks /home/spark && \
    chmod 755 /usr/local/share/jupyter/kernels/pyspark-lakehouse

USER spark

WORKDIR /opt/notebooks

EXPOSE 8888

CMD ["jupyter", "lab"]
