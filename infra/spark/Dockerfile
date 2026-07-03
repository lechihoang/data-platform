FROM apache/spark:4.0.0

# Switch to root to install packages globally
USER root

# Install Python dependencies using standard pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Spark config
COPY config/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY config/log4j2.properties /opt/spark/conf/log4j2.properties

# Switch back to spark user
USER 185
