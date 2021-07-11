public_key_path = "~/.ssh/fluvio_aws.pub"
region          = "us-west-2"
az              = "us-west-2a"
ami             = "ami-03d5c68bab01f3496" // Ubuntu Server 20.04 LTS (HVM), SSD Volume Type

instance_types = {
  "fluvio"     = "i3.4xlarge"
}

num_instances = {
  "fluvio"    = 1
}
