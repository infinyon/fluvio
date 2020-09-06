FLUVIO_BIN=../target/debug/fluvio


clean-cert:
	mkdir -p certs
	rm -rf certs/*


# create secret for k8 in development mode
k8-create-secret:
	kubectl delete secret fluvio-ca --ignore-not-found=true
	kubectl delete secret fluvio-tls --ignore-not-found=true
	kubectl create secret generic fluvio-ca --from-file certs/ca.crt
	kubectl create secret tls fluvio-tls --cert certs/server.crt --key certs/server.key


k8-tls-list:
	$(FLUVIO_BIN) topic list --tls --enable-client-cert --ca-cert tls/certs/ca.crt --client-cert tls/certs/client.crt --client-key tls/certs/client.key --sc fluvio.local:9003

# set k8 profile using tls
k8-set-k8-profile-tls:
	$(FLUVIO_BIN) profile set-k8-profile --tls --domain fluvio.local --enable-client-cert --ca-cert tls/certs/ca.crt --client-cert tls/certs/client.crt --client-key tls/certs/client.key


install-local-tls:
	$(FLUVIO_BIN) cluster install --local  \
		--tls --server-cert tls/certs/server.crt --server-key tls/certs/server.key \
		--ca-cert tls/certs/ca.crt --client-cert tls/certs/client.crt	\
		--client-key tls/certs/client.key --domain fluvio.local
	
uninstall-local:
	$(FLUVIO_BIN) cluster uninstall --local

install-k8-tls:
	$(FLUVIO_BIN) cluster install --develop \
		--tls --server-cert tls/certs/server.crt --server-key tls/certs/server.key \
		--ca-cert tls/certs/ca.crt --client-cert tls/certs/client.crt	\
		--client-key tls/certs/client.key --domain fluvio.local

uninstall-k8:
	$(FLUVIO_BIN) cluster uninstall




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

