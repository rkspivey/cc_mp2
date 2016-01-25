create table "airport_carrier_ontime_departs" (
	"origin" text,
	"airline_id" text,
	"ontime_departs" int,
	"origin_name" text,
	"airline_name" text,
	PRIMARY KEY ("origin", "airline_id", "ontime_departs")
);

create table "airport_dest_ontime_departs" (
	"origin" text,
	"destination" text,
	"ontime_departs" int,
	"origin_name" text,
	"destination_name" text,
	PRIMARY KEY ("origin", "destination", "ontime_departs")
);

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

create table "source_dest_schedules" (
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
	PRIMARY KEY ("airline_id", "flight_date", "flight_num")
);