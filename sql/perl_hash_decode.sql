SET search_path TO smartsig;
DROP TABLE bt_xml_segment cascade;
DROP TABLE bt_xml_location_checkin cascade;
DROP TABLE bt_xml_location cascade;
DROP TABLE bt_xml_project cascade;

CREATE TABLE bt_xml_project (
  projectid    integer primary key,
  title        VARCHAR(128)
);

CREATE TABLE bt_xml_location (
  latitude     numeric,
  locationid   integer primary key,
  locationname VARCHAR(128),
  longitude    numeric,
  projectid    integer REFERENCES bt_xml_project (projectid)
);

CREATE TABLE bt_xml_location_checkin (
  active INTEGER,
  lastcheckin  timestamp with time zone not null,
  locationid   integer REFERENCES bt_xml_location (locationid),
  primary key(locationid,lastcheckin)
);

CREATE TABLE bt_xml_segment (
  segmentid      integer primary key,
  fromlocationid integer REFERENCES bt_xml_location (locationid),
  tolocationid   integer REFERENCES bt_xml_location (locationid),
  route          varchar(128),
  groupby        integer,
  projectid      integer REFERENCES bt_xml_project (projectid),
  ts    timestamp with time zone not null,
  numtrips       integer,
  speed          numeric,
  distance           numeric,
  estimatedtimetaken numeric,
  traveltime         numeric
);



CREATE OR REPLACE FUNCTION perl_xml_segment_decoder (TEXT) RETURNS bt_xml_segment AS $$
    use strict;
    my $unescape = sub {
        my $escaped = shift;
        $escaped =~ s/%u([0-9a-f]{4})/chr(hex($1))/eig;
        $escaped =~ s/%([0-9a-f]{2})/chr(hex($1))/eig;
        return $escaped;
    };

    my $chars = $unescape->( $_[0] );
    my $VAR1;
    eval($chars);
    # clean up some entries we are not using
    my $segment = $VAR1->{'segment'};
$segment->{'ts'} = $segment->{'Timestamp'};
my %bar = map { lc $_ => $segment->{$_} } qw{
  SegmentID
  FromLocationID
  ToLocationID
  Route
  GroupBy
  ProjectID
  ts
  NumTrips
  Speed
  Distance
  EstimatedTimeTaken
  TravelTime
};
    return \%bar;
$$ LANGUAGE plperl;


CREATE OR REPLACE FUNCTION perl_xml_location_decoder_from_location (TEXT) RETURNS bt_xml_location AS $$
    use strict;
    my $unescape = sub {
        my $escaped = shift;
        $escaped =~ s/%u([0-9a-f]{4})/chr(hex($1))/eig;
        $escaped =~ s/%([0-9a-f]{2})/chr(hex($1))/eig;
        return $escaped;
    };

    my $chars = $unescape->( $_[0] );
    my $VAR1;
    eval($chars);
    # clean up some entries we are not using
    my $location = $VAR1->{'location'};
my %bar = map { lc $_ => $location->{$_} } qw{
  Latitude
  LocationID
  LocationName
  Longitude
};
    return \%bar;
$$ LANGUAGE plperl;

CREATE OR REPLACE FUNCTION perl_xml_location_decoder_from_segment (TEXT) RETURNS setof bt_xml_location AS $$
    use strict;
    my $unescape = sub {
        my $escaped = shift;
        $escaped =~ s/%u([0-9a-f]{4})/chr(hex($1))/eig;
        $escaped =~ s/%([0-9a-f]{2})/chr(hex($1))/eig;
        return $escaped;
    };

    my $chars = $unescape->( $_[0] );
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
        'projectid'  => $seg->{'ProjectID'},
    };
    return_next $from;
    return_next $to;
    return undef;
$$ LANGUAGE plperl;
