#!/bin/sh

which aws 2> /dev/null > /dev/null || {
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip";
    unzip awscliv2.zip;
    sudo ./aws/install;
}