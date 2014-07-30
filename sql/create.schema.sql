SET search_path TO smartsig;
drop if exists smartsig.bluetooth_data;
CREATE TABLE smartsig.bluetooth_data  (
 id serial primary key,
 ts timestamp with time zone not null,
 radar_lane_id integer,
 station_lane_id integer,
 name varchar(128),
 route varchar(128),
 direction varchar(4),
 postmile numeric,
 enabled integer,
 firmware varchar(128),
 sample_interval numeric,
 lastpolltime timestamp with time zone not null,
 lastgoodpoll timestamp with time zone not null,
 speed numeric,
 speed_units varchar(32),
 xmlrecord xml
);
