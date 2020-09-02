FROM nginx

WORKDIR /nginx/

# Copy nginx configuration
COPY nginx/nginx.conf /etc/nginx/nginx.conf

# Copy server cert and key
COPY tls/certs/server.crt .
COPY tls/certs/server.key .

# Copy client cert and key
COPY tls/certs/client.crt .
COPY tls/certs/client.key .

# Copy CA cert
COPY tls/certs/ca.crt .

# Copy sample webpage
COPY nginx/index.html /etc/nginx/html/index.html
