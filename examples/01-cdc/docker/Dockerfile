# Derived from official mysql image
FROM mysql:8.0

# Environment variables
ENV MYSQL_ROOT_PASSWORD root

# Copy scripts to be automatically executed by docker
COPY scripts/ /docker-entrypoint-initdb.d/
