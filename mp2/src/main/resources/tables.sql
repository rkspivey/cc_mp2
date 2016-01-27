create keyspace "mp2" with replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

create table "airport_carrier_ontime" (
	"origin" text,
	"airline_id" text,
	"ontime_percentage" double,
	"origin_name" text,
	"airline_name" text,
	PRIMARY KEY (("origin"), "ontime_percentage", "airline_id")
) with clustering order by ("ontime_percentage" desc);

create table "airport_dest_ontime" (
	"origin" text,
	"destination" text,
	"ontime_percentage" double,
	"origin_name" text,
	"destination_name" text,
	PRIMARY KEY (("origin"), "ontime_percentage", "destination")
) with clustering order by ("ontime_percentage" desc);

create table "source_dest_carrier_ontime_arrivals" (
	"origin" text,
	"destination" text,
	"airline_id" text,
	"ontime_arrivals" int,
	"origin_name" text,
	"destination_name" text,
	"airline_name" text,
	PRIMARY KEY ("origin", "destination", "airline_id", "ontime_arrivals")
);

create table "connecting_flights" (
	"flight_num1" int,
	"flight_num2" int,
	"origin" text,
	"layover" text,
	"destination" text,
	"origin_flight_date" int,
	"origin_depart_time" int,
	"origin_arrival_time" int,
	"destination_arrival_delay" int,
	"destination_flight_date" int,
	"destination_depart_time" int,
	"destination_arrival_time" int,
	"destination_arrival_delay" int,
	PRIMARY KEY ("origin", "destination", "layover", "flight_num1", "flight_num2")
);


create table "users_with_status_updates" (
	"username" text,
	"id" timeuuid,
	"email" text static,
	"encrypted_password" text static,
	"body" text,
	primary key ("username", "id")
);

insert into "users_with_status_updates" 
("username", "id", "email", "encrypted_password", "body")
values (
'alice', now(), 'alice@gmail.com', 'test', 'Learning Cassandra');

insert into "users_with_status_updates" 
("username", "id", "body")
values (
'alice', now(), 'Learning Cassandra Chap 1');

insert into "users_with_status_updates" 
("username", "id", "body")
values (
'alice', now(), 'Learning Cassandra Chap 2');

insert into "users_with_status_updates" 
("username", "id", "body")
values (
'alice', now(), 'Learning Cassandra Chap 3');

create table flights_with_stopover (
	"flight_num" int,
	"origin" text,
	"destination" text,
	"airline_id" text,
	"flight_date" int,
	"depart_time" int,
	"arrival_time" int,
	"origin_name" text,
	"destination_name" text,
	"airline_name" text,	
);