-- Databricks notebook source
-- MAGIC %run ./Ref_var_declare

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(name)
-- MAGIC print(Age)
-- MAGIC print(Salary)

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"My Name is {name} & my age is {Age} years old")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create table emp
-- MAGIC (
-- MAGIC     id int,
-- MAGIC     name string,
-- MAGIC     salary long,
-- MAGIC     dept int
-- MAGIC )

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC insert into emp values(1,'A',100,10);
-- MAGIC insert into emp values(2,'B',100,10);
-- MAGIC insert into emp values(3,'C',100,30);
-- MAGIC insert into emp values(4,'D',100,20);
-- MAGIC insert into emp values(5,'E',100,20);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe detail emp

-- COMMAND ----------

describe extended emp;

-- COMMAND ----------

update emp 
set salary = salary +1000

-- COMMAND ----------

describe history emp;

-- COMMAND ----------

select * from emp version as of 6

-- COMMAND ----------

optimize emp
zorder by (id)

-- COMMAND ----------

desc history emp

-- COMMAND ----------

describe detail dept

-- COMMAND ----------

vacuum emp

-- COMMAND ----------

create table dept( deptid int, deptname string ,mgr string)

-- COMMAND ----------

/* insert into dept values(1,'HR','A');
insert into dept values(1,'HR','A');
insert into dept values(1,'HR','A');
insert into dept values(1,'HR','A'); */
update dept set mgr="digambar";

-- COMMAND ----------

--describe history dept;
restore dept version as of 5;

select * from dept

-- COMMAND ----------

optimize dept 
zorder by (deptid);

-- COMMAND ----------

vacuum dept

-- COMMAND ----------

create database emp_db

-- COMMAND ----------

create table emp_db.emp (
  id int, name string
);

-- COMMAND ----------

use emp_db;
insert into emp values(1,'A');
insert into emp values(1,'A');
insert into emp values(1,'A');
insert into emp values(1,'A');
insert into emp values(1,'A');

-- COMMAND ----------

use emp_db;
select * from emp;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/emp_db.db/emp'

-- COMMAND ----------

use emp_db;
create table external_emp(
  id int,
  name string
)
location 'dbfs:/mnt/demo/emp';

-- COMMAND ----------

insert into external_emp values (1,'X');
select * from external_emp;

-- COMMAND ----------

describe history emp;

-- COMMAND ----------

drop table external_emp;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/emp'

-- COMMAND ----------

use emp_db;

--restore table emp_db.external_emp version as of 5
--select * from emp_db.emp;

--create table external_emp
--as select * from emp;

--select * from external_emp;

create table emp_db.ext_dept (
  deptid int,
  dname string,
  mgr string
)
location 'dbfs:/mnt/demo/ext_dept/';


-- COMMAND ----------

describe extended emp_db.ext_dept;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/'
-- MAGIC
-- MAGIC /*insert into ext_dept values(1,'HR','X');
-- MAGIC insert into ext_dept values(4,'HRA','Q');
-- MAGIC insert into ext_dept values(3,'HRC','P');
-- MAGIC insert into ext_dept values(2,'HRD','Y');
-- MAGIC */
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC path="dbfs:/mnt/dbacademy-datasets/data-engineer-learning-path/v01/"
-- MAGIC display(dbutils.fs.ls('{path}'))
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
