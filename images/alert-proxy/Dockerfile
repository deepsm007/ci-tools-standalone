FROM registry.access.redhat.com/ubi9/ubi-minimal:latest
LABEL maintainer="DPTP"

ADD alert-proxy /usr/bin/alert-proxy
ENTRYPOINT ["/usr/bin/alert-proxy"]
