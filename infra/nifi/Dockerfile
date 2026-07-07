FROM apache/nifi:2.0.0

USER root
COPY entrypoint.sh /opt/nifi/scripts/entrypoint.sh
RUN chmod +x /opt/nifi/scripts/entrypoint.sh
USER nifi

ENTRYPOINT ["/opt/nifi/scripts/entrypoint.sh"]
