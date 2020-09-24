CREATE USER 'fluvio'@'%' IDENTIFIED WITH mysql_native_password BY 'fluvio4cdc!';
GRANT ALL PRIVILEGES ON `flvDb%`.* TO 'fluvio'@'%';
FLUSH PRIVILEGES;