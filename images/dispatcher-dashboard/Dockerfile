FROM registry.access.redhat.com/ubi9/ubi-minimal:latest

COPY dispatcher-dashboard /usr/bin/dispatcher-dashboard
USER 1001
EXPOSE 8080
ENTRYPOINT ["/usr/bin/dispatcher-dashboard"]
CMD ["--listen=127.0.0.1:8080"]
