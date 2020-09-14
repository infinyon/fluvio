# Start a single SPU with id of 5001, ports of 9005, 9006

kubectl create -f k8-util/samples/crd/spu_5001.yaml 
kubectl create -f k8-util/samples/crd/spu_5002.yaml 
kubectl create -f k8-util/samples/crd/spu_5003.yaml 

./dev-tools/log/debug-spu-min 5001 9005 9006
./dev-tools/log/debug-spu-min 5002 9007 9008
./dev-tools/log/debug-spu-min 5003 9009 9010