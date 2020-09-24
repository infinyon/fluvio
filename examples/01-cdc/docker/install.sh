#!/bin/bash

helpFunction()
{
   echo ""
   echo "Usage: $0 -n name -d data-path -p port"
   echo -e "\t-n Container name"
   echo -e "\t-d Data path"
   echo -e "\t-p Port number"
   exit 1 # Exit script after printing help
}

while getopts "n:d:p:" opt
do
   case "$opt" in
      n ) name="$OPTARG" ;;
      d ) path="$OPTARG" ;;
      p ) port="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

# Print helpFunction in case parameters are empty
if [ -z "$name" ] || [ -z "$path" ] || [ -z "$port" ]
then
   echo "Invalid parameters";
   helpFunction
fi

# Create directory
mkdir -p $path

# Build iamge
docker build . -t mysql-80

# Run Image
docker run -p $port:3306 \
    -v $path:/var/lib/mysql \
    -v scripts:/docker-entrypoint-initdb.d/ \
    --name $name \
    -e MYSQL_ROOT_PASSWORD=root \
    -d mysql-80 \
    --server-id=1 \
    --log-bin=/var/lib/mysql/binlog.index \
    --binlog-format=row \
    --default-authentication-plugin=mysql_native_password