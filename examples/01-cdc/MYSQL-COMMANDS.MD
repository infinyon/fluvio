# Mysql Sample Commands

Sample command to demonstrate event propagation between two mysql instances.

Fluvio producer/consumer can propagate the following commands:
* CREATE database
* DROP database
* ALTER table
* INSERT into table
* UPDATE table rows
* DELETE table rows

Each command is translated into an event that is transmitted to slave instances in real-time.

## Database - flvDb

```
CREATE DATABASE flvDb;
```

```
USE flvDb;
```

## Table - Pet

```
CREATE TABLE pet (name VARCHAR(20), owner VARCHAR(20), species VARCHAR(20), sex CHAR(1), birth DATE);
```

```
INSERT INTO pet VALUES ('Puffball','Diane','hamster','f','1999-03-30');
```

```
INSERT INTO pet VALUES ('Jack','Peter','dog','m','1999-03-30');
```

```
UPDATE pet SET birth = '1989-08-31' WHERE name = 'Jack';
```

```
ALTER TABLE pet ADD COLUMN death DATE;
```

```
DELETE from pet where name="Puffball";
```

```
INSERT INTO pet VALUES ("Spot", "Jane", "dog", "m", "2010-11-2", Null);
```

```
UPDATE pet SET death="2020-6-10" WHERE name="Spot";
```

## Table - User
```
CREATE TABLE user (first_name VARCHAR(20), last_name VARCHAR(20),  sex CHAR(1), birth DATE);
```

```
INSERT INTO user VALUES ('John','Doe','m','2000-03-30');
```


## Table - Year

```
CREATE TABLE year (y YEAR);
```

```
INSERT INTO year (y) VALUES (1998);
```

```
INSERT INTO year (y) VALUES (1999);
```


```
INSERT INTO year (y) VALUES (2009);
```

```
INSERT INTO year (y) VALUES (2020);
```

```
DELETE FROM year  WHERE y LIKE "19%";
```

```
DROP TABLE year;
```