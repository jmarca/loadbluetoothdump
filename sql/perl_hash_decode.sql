SET search_path TO smartsig;
DROP TABLE bt_xml_segment cascade;
DROP TABLE bt_xml_location_checkin cascade;
DROP TABLE bt_xml_observation cascade;
DROP TABLE bt_xml_location cascade;
DROP TABLE bt_xml_project cascade;

CREATE TABLE bt_xml_project (
  projectid    integer primary key,
  title        VARCHAR(128)
);

CREATE TABLE bt_xml_location (
  locationid   integer primary key,
  locationname VARCHAR(128),
  latitude     numeric,
  longitude    numeric,
  projectid    integer not null REFERENCES bt_xml_project (projectid)
);

CREATE TABLE bt_xml_location_checkin (
  active INTEGER,
  lastcheckin  timestamp with time zone not null,
  locationid   integer not null  REFERENCES bt_xml_location (locationid),
  primary key(locationid,lastcheckin)
);

CREATE TABLE bt_xml_segment (
  segmentid      integer primary key,
  fromlocationid integer not null REFERENCES bt_xml_location (locationid),
  tolocationid   integer not null REFERENCES bt_xml_location (locationid),
  route          varchar(128),
  groupby        integer,
  projectid      integer not null REFERENCES bt_xml_project (projectid)
);

CREATE TABLE bt_xml_observation(
  segmentid      integer not null references bt_xml_segment(segmentid),
  ts    timestamp with time zone not null,
  data_ts timestamp with time zone not null,
  radar_lane_id integer,
  station_lane_id integer,
  numtrips       integer,
  speed          numeric,
  distance           numeric,
  estimatedtimetaken numeric,
  traveltime         numeric,
  primary key(segmentid,ts,data_ts,radar_lane_id,station_lane_id),
  foreign key (data_ts,radar_lane_id,station_lane_id) references smartsig.bluetooth_data(ts,radar_lane_id,station_lane_id)
);

alter table smartsig.bt_xml_observation
      add constraint datats_radar_station_idx
          unique(data_ts,radar_lane_id,station_lane_id)  ;

CREATE VIEW smartsig.bt_xml_and_data as
select a.*,b.id,b.name,b.route,b.direction,b.postmile,b.enabled,b.firmware,
       b.sample_interval,b.lastpolltime,b.lastgoodpoll,b.speed as speed_value,b.speed_units,
       s.route as segmentroute,s.groupby,
       f.locationname as from_name,
       f.latitude as from_latitude,
       f.longitude as from_longitude,
       t.locationname as to_name,
       t.latitude as to_latitude,
       t.longitude as to_longitude
from smartsig.bt_xml_observation a
join smartsig.bluetooth_data b ON (
                             a.data_ts=b.ts and
                             a.radar_lane_id=b.radar_lane_id and
                             a.station_lane_id=b.station_lane_id)

join smartsig.bt_xml_segment s USING(segmentid)
join smartsig.bt_xml_location f ON(fromlocationid=f.locationid)
join smartsig.bt_xml_location t ON(tolocationid=t.locationid)
;

CREATE OR REPLACE FUNCTION perl_xml_segment_decoder () RETURNS setof bt_xml_segment AS $$
    use strict;
    my $unescape = sub {
        my $escaped = shift;
        $escaped =~ s/%u([0-9a-f]{4})/chr(hex($1))/eig;
        $escaped =~ s/%([0-9a-f]{2})/chr(hex($1))/eig;
        return $escaped;
    };

    my $sth = spi_query("SELECT * FROM perlhash");
    while ( defined( my $row = spi_fetchrow($sth) ) ) {
        my $chars = $unescape->( $row->{data} );
        my $VAR1;
        eval($chars);

        # clean up some entries we are not using
        my $segment = $VAR1->{'segment'};
        my %bar = map { lc $_ => $segment->{$_} } qw{
          SegmentID
          FromLocationID
          ToLocationID
          Route
          GroupBy
          ProjectID
        };
        return_next \%bar;
    }
    return undef;
$$ LANGUAGE plperl;

CREATE OR REPLACE FUNCTION perl_xml_segment_obs_decoder () RETURNS setof bt_xml_observation AS $$
    use strict;
    my $unescape = sub {
        my $escaped = shift;
        $escaped =~ s/%u([0-9a-f]{4})/chr(hex($1))/eig;
        $escaped =~ s/%([0-9a-f]{2})/chr(hex($1))/eig;
        return $escaped;
    };

    my $sth = spi_query("SELECT * FROM perlhash");
    while ( defined( my $row = spi_fetchrow($sth) ) ) {
        my $chars = $unescape->( $row->{data} );
        my $VAR1;
        eval($chars);

        # clean up some entries we are not using
        my $segment = $VAR1->{'segment'};
        $segment->{'ts'} = $segment->{'Timestamp'};
        my %bar = map { lc $_ => $segment->{$_} } qw{
          SegmentID
          ts
          NumTrips
          Speed
          Distance
          EstimatedTimeTaken
          TravelTime
        };
        $bar{data_ts}=$row->{ts};
        $bar{radar_lane_id}=$row->{radar_lane_id};
        $bar{station_lane_id}=$row->{station_lane_id};
        return_next \%bar;
    }
    return undef;
$$ LANGUAGE plperl;


CREATE OR REPLACE FUNCTION perl_xml_project_decoder_from_location () RETURNS setof bt_xml_project AS $$
    use strict;
    my $unescape = sub {
        my $escaped = shift;
        $escaped =~ s/%u([0-9a-f]{4})/chr(hex($1))/eig;
        $escaped =~ s/%([0-9a-f]{2})/chr(hex($1))/eig;
        return $escaped;
    };

    my $sth = spi_query("SELECT * FROM perlhash AS b(a)");
    while ( defined( my $row = spi_fetchrow($sth) ) ) {
        my $chars = $unescape->( $row->{a} );
        my $VAR1;
        eval($chars);

        # clean up some entries we are not using
        my $location = $VAR1->{'location'};
        my %bar = map { lc $_ => $location->{$_} } qw{
          ProjectID
          Title
        };
        return_next \%bar;
    }
    return undef;
$$ LANGUAGE plperl;

CREATE OR REPLACE FUNCTION perl_xml_location_decoder_from_location () RETURNS setof bt_xml_location AS $$
    use strict;
    my $unescape = sub {
        my $escaped = shift;
        $escaped =~ s/%u([0-9a-f]{4})/chr(hex($1))/eig;
        $escaped =~ s/%([0-9a-f]{2})/chr(hex($1))/eig;
        return $escaped;
    };

    my $sth = spi_query("SELECT * FROM perlhash AS b(a)");
    while ( defined( my $row = spi_fetchrow($sth) ) ) {
        my $chars = $unescape->( $row->{a} );
        my $VAR1;
        eval($chars);

        # clean up some entries we are not using
        my $location = $VAR1->{'location'};
        my %bar = map { lc $_ => $location->{$_} } qw{
          Latitude
          LocationID
          ProjectID
          LocationName
          Longitude
        };
        return_next \%bar;
    }
    return undef;
$$ LANGUAGE plperl;


CREATE OR REPLACE FUNCTION perl_xml_location_decoder_from_segment () RETURNS setof bt_xml_location AS $$
    use strict;
    my $unescape = sub {
        my $escaped = shift;
        $escaped =~ s/%u([0-9a-f]{4})/chr(hex($1))/eig;
        $escaped =~ s/%([0-9a-f]{2})/chr(hex($1))/eig;
        return $escaped;
    };
    my $sth = spi_query("SELECT * FROM perlhash AS b(a)");
    while ( defined(my $row = spi_fetchrow($sth) ) ) {
        my $chars = $unescape->( $row->{a} );
        my $VAR1;
        eval($chars);
        my $seg  = $VAR1->{'segment'};
        my $from = {
            'latitude'     => $seg->{'FromLatitude'},
            'longitude'    => $seg->{'FromLongitude'},
            'locationid'   => $seg->{'FromLocationID'},
            'locationname' => $seg->{'FromLocationName'},
            'projectid'    => $seg->{'ProjectID'},
        };
        my $to = {
            'latitude'   => $seg->{'ToLatitude'},
            'longitude'  => $seg->{'ToLongitude'},
            'locationid' => $seg->{'ToLocationID'},
            'locationname' => $seg->{'ToLocationName'},
            'projectid'  => $seg->{'ProjectID'},
        };
        return_next $from;
        return_next $to;
    }
    return undef;
$$ LANGUAGE plperl;
