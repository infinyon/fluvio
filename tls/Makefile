
clean-cert:
	mkdir -p certs
	rm -rf certs/*


generate-certs:	clean-cert	generate-ca-crt generate-server-crt generate-client-crt

generate-ca-key:	
	openssl genrsa  -out certs/ca.key 4096


# this is CA pem file
generate-ca-crt:	generate-ca-key
	openssl req -x509 -new -nodes -key certs/ca.key  -days 1825 -out certs/ca.crt \
		-subj /C=US/ST=CA/L=Sunnyvale/O=Fluvio/OU=Eng/CN=fluvio.io


generate-server-key:	
	openssl genrsa -out certs/server.key 4096


generate-server-csr:	generate-server-key
	openssl req -new -key certs/server.key \
		-out certs/server.csr \
		-config  cert.conf 


verify-csr:
	openssl req -in certs/server.csr -noout -text

decrypt-server-crt:
	openssl x509 -in certs/server.crt   -noout -text

generate-server-crt:	generate-server-csr
	openssl x509 -req \
		-in certs/server.csr \
		-out certs/server.crt \
		-CA certs/ca.crt \
		-CAkey certs/ca.key \
		-CAcreateserial  \
		-days 500 \
		-extensions v3_end \
		-extfile openssl.cnf


generate-client-key:
	openssl genrsa -out certs/client.key 4096

generate-client-csr:	generate-client-key
	openssl req -new -key certs/client.key -out certs/client.csr \
		-subj "/C=US/ST=CA/O=MyOrg, Inc./CN=client.com"

generate-client-crt:	generate-client-csr
	openssl x509 -req \
		-days 365 -in certs/client.csr \
		-out certs/client.crt \
		-CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial \
		-extensions v3_end \
		-extfile openssl.cnf



# for non mac
test-curl:
	curl -v -s -k --key client.key --cert client.crt "https://127.0.0.1:8443/hello/world"

install-curl-ssl:
	 brew upgrade curl-openssl

test-mac-curl:
	 /usr/local/opt/curl-openssl/bin/curl -v -k -s --key certs/client.key --cert certs/client.crt "https://127.0.0.1:8443/hello/world"


MAKE_DIR = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))

start-nginx:
	nginx -c $(MAKE_DIR)/nginx.conf 

stop-nginx:
	nginx -s quit

