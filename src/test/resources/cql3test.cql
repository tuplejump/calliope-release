DROP KEYSPACE cql3_test;

CREATE KEYSPACE cql3_test WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '3'
};

USE cql3_test;

CREATE TABLE emp_read_test (
  deptid int,
  empid int,
  first_name text,
  last_name text,
  PRIMARY KEY (deptid, empid)
);

CREATE TABLE emp_write_test (
  deptid int,
  empid int,
  first_name text,
  last_name text,
  PRIMARY KEY (deptid, empid)
);

CREATE INDEX firstNameIndex ON emp_read_test (first_name);

insert into emp_read_test(deptid, empid, first_name, last_name) values(20,105, 'jack', 'carpenter');
insert into emp_read_test(deptid, empid, first_name, last_name) values(20, 106, 'john',  'grumpy');
insert into emp_read_test(deptid, empid, first_name, last_name) values(21,   105, 'james', 'dawson');
insert into emp_read_test(deptid, empid, first_name, last_name) values(21, 108, 'alice', 'mathews');
insert into emp_read_test(deptid, empid, first_name, last_name) values(21, 110, 'estonio', 'perreze');
