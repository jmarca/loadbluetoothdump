use strict;
use Encode qw(decode encode);
use Carp;
use Data::Dumper;

my $dump =
'%24VAR1%20%3D%20%7B%0A%20%20%27location%27%20%3D%3E%20%7B%0A%20%20%20%20%27Active%27%20%3D%3E%201%2C%0A%20%20%20%20%27LastCheckin%27%20%3D%3E%20%272014-07-15T17%3A48%3A00.0000000%27%2C%0A%20%20%20%20%27Latitude%27%20%3D%3E%20%2733.8736838%27%2C%0A%20%20%20%20%27LocationID%27%20%3D%3E%203481%2C%0A%20%20%20%20%27LocationName%27%20%3D%3E%20%27SR-39%20at%20Artesia%20Blvd%27%2C%0A%20%20%20%20%27Longitude%27%20%3D%3E%20%27-117.99818085%27%2C%0A%20%20%20%20%27ProjectID%27%20%3D%3E%20672%2C%0A%20%20%20%20%27Title%27%20%3D%3E%20%27Beach%20Blvd%20%28SR-39%29%27%0A%20%20%7D%2C%0A%20%20%27segment%27%20%3D%3E%20%7B%0A%20%20%20%20%27Distance%27%20%3D%3E%20%270.8%27%2C%0A%20%20%20%20%27EstimatedTimeTaken%27%20%3D%3E%20%2786%27%2C%0A%20%20%20%20%27FromLatitude%27%20%3D%3E%20%2733.8736838%27%2C%0A%20%20%20%20%27FromLocationID%27%20%3D%3E%203481%2C%0A%20%20%20%20%27FromLocationName%27%20%3D%3E%20%27SR-39%20at%20Artesia%20Blvd%27%2C%0A%20%20%20%20%27FromLongitude%27%20%3D%3E%20%27-117.99818085%27%2C%0A%20%20%20%20%27GroupBy%27%20%3D%3E%2015%2C%0A%20%20%20%20%27NumTrips%27%20%3D%3E%2014%2C%0A%20%20%20%20%27ProjectID%27%20%3D%3E%20672%2C%0A%20%20%20%20%27Route%27%20%3D%3E%20%27SR-39%27%2C%0A%20%20%20%20%27SegmentID%27%20%3D%3E%204558%2C%0A%20%20%20%20%27Speed%27%20%3D%3E%20%278.04274565301844%27%2C%0A%20%20%20%20%27Timestamp%27%20%3D%3E%20%272014-07-15T17%3A30%3A00.0000000%27%2C%0A%20%20%20%20%27Title%27%20%3D%3E%20%27Beach%20Blvd%20%28SR-39%29%27%2C%0A%20%20%20%20%27ToLatitude%27%20%3D%3E%20%2733.8844892743301%27%2C%0A%20%20%20%20%27ToLocationID%27%20%3D%3E%203472%2C%0A%20%20%20%20%27ToLocationName%27%20%3D%3E%20%27SR-39%20at%20La%20Mirada%20Blvd%27%2C%0A%20%20%20%20%27ToLongitude%27%20%3D%3E%20%27-117.995417118073%27%2C%0A%20%20%20%20%27TravelTime%27%20%3D%3E%20%27356.285714285714%27%0A%20%20%7D%0A%7D%3B%0A';

my $unescape = sub {
    my $escaped = shift;
    $escaped =~ s/%u([0-9a-f]{4})/chr(hex($1))/eig;
    $escaped =~ s/%([0-9a-f]{2})/chr(hex($1))/eig;
    return $escaped;
};

my $chars = $unescape->($dump);
my $VAR1;
eval($chars);

# cleanup
$VAR1->{segment}->{'ts'} = $VAR1->{'segment'}->{'Timestamp'};
my %bar = map { lc $_ => $VAR1->{'segment'}->{$_} } qw{
  SegmentID
  FromLocationID
  ToLocationID
  Route
  Title
  GroupBy
  ProjectID
  ts
  NumTrips
  Speed
  Distance
  EstimatedTimeTaken
  TravelTime
};

  carp Dumper( \%bar );


    my $seg  = $VAR1->{'segment'};
    my $from = {
        'latitude'     => $seg->{'FromLatitude'},
        'longitude'    => $seg->{'FromLongitude'},
        'locationid'   => $seg->{'FromLocationID'},
        'locationname' => $seg->{'LocationName'},
    };
    my $to = {
        'latitude'   => $seg->{'ToLatitude'},
        'longitude'  => $seg->{'ToLongitude'},
        'locationid' => $seg->{'ToLocationID'},
    };

carp Dumper( [$from,$to]);

1;
