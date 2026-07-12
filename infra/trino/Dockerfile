FROM trinodb/trino:475

# Copy Trino server config
COPY config.properties /etc/trino/config.properties
COPY node.properties /etc/trino/node.properties
COPY jvm.config /etc/trino/jvm.config
COPY openlineage-event-listener.properties /etc/trino/openlineage-event-listener.properties

# Copy catalog config (Nessie/Iceberg + MinIO)
COPY catalog/nessie.properties /etc/trino/catalog/nessie.properties
