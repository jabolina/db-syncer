create schema debezium;
create table debezium.customer(id int not null, fullname varchar(255), email varchar(255), constraint primary key (id));
create table debezium.car_model(id int not null, model varchar(255), brand varchar(255), owner int, constraint primary key (id), foreign key (owner) references debezium.customer(id));
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'gingersnap_user';
