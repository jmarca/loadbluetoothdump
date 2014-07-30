SET search_path TO smartsig;
DROP TABLE bt_xml_segment cascade;
DROP TABLE bt_xml_location cascade;

CREATE TABLE bt_xml_location (
  Active INTEGER,
  LastCheckin  timestamp with time zone not null,
  Latitude     numeric,
  LocationID   integer primary key,
  LocationName VARCHAR(128),
  Longitude    numeric,
  ProjectID    integer,
  Title        VARCHAR(128)
);

CREATE TABLE bt_xml_segment (
  SegmentID      integer primary key,
  FromLocationID integer REFERENCES bt_xml_location (LocationID),
  ToLocationID   integer REFERENCES bt_xml_location (LocationID),
  Route          varchar(128),
  Title          varchar(128),
  GroupBy        integer,
  ProjectID      integer,
  Timestamp      timestamp with time zone not null,
  NumTrips       integer,
  Speed          numeric,
  Distance           numeric,
  EstimatedTimeTaken numeric,
  TravelTime         numeric
);

--CREATE OR REPLACE FUNCTION perl_xml_hash_decoder (TEXT) RETURNS SETOF bt_xml_type AS $$
