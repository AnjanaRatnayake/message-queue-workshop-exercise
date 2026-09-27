# The Antithesis "config image": a scratch image whose only job is to carry
# docker-compose.yaml. Build and push it under your own tag:
#
#   docker build -t us-central1-docker.pkg.dev/molten-verve-216720/TENANT_NAME-repository/kafka-alo-config:YOURNAME-m01 .
#   docker push     us-central1-docker.pkg.dev/molten-verve-216720/TENANT_NAME-repository/kafka-alo-config:YOURNAME-m01
FROM scratch
COPY docker-compose.yaml /docker-compose.yaml
