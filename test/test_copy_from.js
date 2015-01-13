/* global require console process describe it */

var should = require('should')

var path    = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'

var config_okay = require('config_okay')

var config={}


var pg = require('pg')
var rw = require ('rw')
var os = require('os')

var queue = require("queue-async")

var temp_table_query

var prepare_temp_tables = function (client, callback) {

    // I have a number of temp tables to create
    var bt_xml_project = "CREATE TEMP TABLE bt_xml_project ( projectid    integer primary key, title VARCHAR(128))"

    var bt_xml_location = "CREATE TEMP TABLE bt_xml_location (  latitude     numeric,  locationid   integer primary key,  locationname VARCHAR(128),  longitude    numeric,  projectid    integer not null REFERENCES bt_xml_project (projectid))"

    var bt_xml_location_checkin = "CREATE TEMP TABLE bt_xml_location_checkin (  active INTEGER,  lastcheckin  timestamp with time zone not null,  locationid   integer not null  REFERENCES bt_xml_location (locationid),  primary key(locationid,lastcheckin))"

    var bt_xml_segment = "CREATE TEMP TABLE bt_xml_segment (  segmentid      integer primary key,  fromlocationid integer not null REFERENCES bt_xml_location (locationid),  tolocationid   integer not null REFERENCES bt_xml_location (locationid),  route          varchar(128),  groupby        integer,  projectid      integer not null REFERENCES bt_xml_project (projectid))"

    var bt_xml_observation = "CREATE TEMP TABLE bt_xml_observation(  segmentid      integer not null references bt_xml_segment(segmentid),  ts    timestamp with time zone not null,  data_ts timestamp with time zone not null,  radar_lane_id integer,  station_lane_id integer,  numtrips       integer,  speed          numeric,  distance           numeric,  estimatedtimetaken numeric,  traveltime         numeric,  primary key(segmentid,ts,data_ts,radar_lane_id,station_lane_id))"

    var create_statements = [temp_table_query
                             ,bt_xml_project
                             ,bt_xml_location
                             ,bt_xml_location_checkin
                             ,bt_xml_segment
                             ,bt_xml_observation
                            ]

    var q = queue(1);
    q.defer(function(cb){
        bt_parser.set_search_path(client,cb)
    })
    create_statements.forEach(function(statement) {
        q.defer(function(cb){
            client.query(statement
                         ,function (err, result) {
                             //console.log(err)
                             should.not.exist(err)
                             return cb(err)
                         })
        })
        return null
    })
    q.awaitAll(function(error, results) {
        //console.log("all done with db temp table creation")
        return callback()
    })

    return null
}

//var bt_parser = require('../.')
var bt_parser=require('../lib/parser')

before(function(done){
    config_okay(config_file,function(err,c){

        // should start using node-postgres/lib/connection-parameters.js here
        // which appears to default to the pg convention of using PGUSER, PGPASS, etc in env vars

        if(!c.postgresql.db){ throw new Error('need valid postgresql.db defined in test.config.json')}
        if(!c.postgresql.table){ throw new Error('need valid postgresql.table defined in test.config.json')}
        if(!c.postgresql.user){ throw new Error('need valid postgresql.user defined in test.config.json')}
        if(!c.postgresql.pass){ throw new Error('need valid postgresql.pass defined in test.config.json')}

        // sane defaults
        if(c.postgresql.host === undefined) c.postgresql.host = 'localhost'
        if(c.postgresql.port === undefined) c.postgresql.port = 5432

        config = c

        temp_table_query = [
            'CREATE TEMP TABLE '
          ,c.postgresql.table
          ,'('
          ,'id serial primary key,'
          ,'ts timestamp with time zone not null,'
          ,'radar_lane_id integer,  '
          ,'station_lane_id integer,'
          ,'name varchar(128),'
          ,'route varchar(128),'
          ,'direction varchar(4),'
          ,'postmile numeric,'
          ,'enabled integer,'
          ,'firmware varchar(128),'
          ,'sample_interval numeric,'
          ,'lastpolltime timestamp with time zone not null,'
          ,'lastgoodpoll timestamp with time zone not null,'
          ,'speed numeric,'
          ,'speed_units varchar(32),'
          ,'UNIQUE (ts,radar_lane_id,station_lane_id)'
          ,')'
        ].join('')

        var connstring = "pg://"
                       + c.postgresql.user+":"
                       + c.postgresql.pass+"@"
                       + c.postgresql.host+":"
                       + c.postgresql.port+"/"
                       + c.postgresql.db
        config.connstring = connstring
        return done()
    })
    return null
})

describe('copy data into db',function(){

    it('should work for one file',function(done){
        pg.connect(config.connstring, function (err, client, client_done) {
            should.not.exist(err)
            var lines=0
            prepare_temp_tables(client, function () {
                var copy_statement = bt_parser.copy_statement(config.postgresql.table)
                //console.log(copy_statement)
                var writer = client.copyFrom( copy_statement );

                var _reader = rw.fileReader
                ("test/bluetooth_log-2014-07-28-21\:00\:00.048") //bit file1210 records
                //("test/bluetoothdump") // small file, 50 records
                var parser_instance

                writer.on('error', function (error) {
                    console.log("Sorry, error happens", error);
                    throw new Error("COPY FROM stream should not emit errors" + JSON.stringify(error))
                });

                writer.on('close',function(error){
                    //console.log("Data inserted sucessfully");
                    should.not.exist(error)

                    // stash away perl strings
                    var create_statement = bt_parser.create_perlhash_statement('perlhash')
                    client.query(create_statement,function(e,r){
                        //console.log(e)
                        should.not.exist(e)
                        //console.log('created perlhash temp table')
                        var copy_statement_perl = bt_parser.copy_perlhash_statement('perlhash')
                        var perlwriter = client.copyFrom( copy_statement_perl );
                        parser_instance.perl_write(perlwriter)
                        perlwriter.on('close',function(err){
                            parser_instance.perl_parser(client,function(e){

                                var q = queue(5);
                                // setup tests
                                var tasks=[]
                                tasks.push(function(callback){
                                    client.query('select * from '+config.postgresql.table,function(e,d){
                                        should.not.exist(e)
                                        should.exist(d)
                                        d.should.have.property('rows').with.lengthOf (1210)
                                        //console.log(d.rows.length)
                                        d.rows.forEach(function(row,i){
                                            row.should.have.keys(
                                                'id'
                                                ,'ts'
                                                ,'radar_lane_id'
                                                ,'station_lane_id'
                                                ,'name'
                                                ,'route'
                                                ,'direction'
                                                ,'postmile'
                                                ,'enabled'
                                                ,'firmware'
                                                ,'sample_interval'
                                                ,'lastpolltime'
                                                ,'lastgoodpoll'
                                                ,'speed'
                                                ,'speed_units'
                                            )
                                        })
                                        return callback()
                                    })
                                })
                                tasks.push(function(callback){
                                    client.query('select * from perlhash',function(e,d){
                                        should.not.exist(e)
                                        should.exist(d)
                                        d.should.have.property('rows').with.lengthOf(1210)
                                        //console.log(d.rows.length)

                                        return callback()
                                    })
                                })

                                tasks.push(function(callback){
                                    client.query('select * from bt_xml_project',function(e,d){
                                        should.not.exist(e)
                                        should.exist(d)
                                        d.should.have.property('rows').with.lengthOf(1)
                                        return callback()
                                    })
                                })
                                tasks.push(function(callback){
                                    client.query('select * from bt_xml_location',function(e,d){
                                        should.not.exist(e)
                                        should.exist(d)
                                        d.should.have.property('rows').with.lengthOf(5)
                                        return callback()
                                    })
                                })
                                tasks.push(function(callback){
                                    client.query('select * from bt_xml_segment',function(e,d){
                                        should.not.exist(e)
                                        should.exist(d)
                                        d.should.have.property('rows').with.lengthOf(8)
                                        return callback()
                                    })
                                })
                                tasks.push(function(callback){
                                    client.query('select * from bt_xml_observation',function(e,d){
                                        should.not.exist(e)
                                        should.exist(d)
                                        d.should.have.property('rows').with.lengthOf(1210)
                                        return callback()
                                    })
                                })

                                tasks.forEach(function(t) { q.defer(t); });
                                q.awaitAll(function(error, results) {
                                    //console.log("all done with db checks")
                                    client_done()
                                    pg.end()
                                    return done()
                                })

                            })
                        })
                    })
                })


                parser_instance=bt_parser(_reader,writer)

                // open a reader

                return null

            })


        });
    });


})

describe('copy lots of data into db',function(){

    it('should work for two files',function(done){
        pg.connect(config.connstring, function (err, client, client_done) {
            should.not.exist(err)
            prepare_temp_tables(client, function () {
                var copy_statement = bt_parser.copy_statement(config.postgresql.table)
                var files = ["test/bluetooth_log-2014-07-28-21\:00\:00.048" //bit file1210 records
                             ,"test/bluetoothdump" // small file, 50 records
                            ]
                var fq = new queue(1)
                fq.defer(function(cb){
                    var create_statement = bt_parser.create_perlhash_statement('perlhash')
                    client.query(create_statement,function(e,r){
                        //console.log(e)
                        should.not.exist(e)
                        return cb(e)
                    })
                })

                function process_file(file,callback){
                    var _reader = rw.fileReader(file)
                    var parser_instance

                    var writer = client.copyFrom( copy_statement );

                    writer.on('error', function (error) {
                        console.log("Sorry, error happens", error);
                        throw new Error("COPY FROM stream should not emit errors" + JSON.stringify(error))
                    });

                    writer.on('close',function(error){
                        //console.log("Data inserted sucessfully");
                        should.not.exist(error)

                        var copy_statement_perl = bt_parser.copy_perlhash_statement('perlhash')
                        var perlwriter = client.copyFrom( copy_statement_perl );
                        parser_instance.perl_write(perlwriter)
                        perlwriter.on('close',function(err){
                            parser_instance.perl_parser(client,function(e){
                                parser_instance.perl_truncate(client,callback)
                                return null
                            })
                        })
                    })
                    parser_instance=bt_parser(_reader,writer)
                }
                files.forEach(function(f){
                    fq.defer(process_file,f)
                    return null
                })

                fq.awaitAll(function(error, results) {
                    var q = queue(5);
                    // setup tests
                    var tasks=[]
                    tasks.push(function(callback){
                        client.query('select * from '+config.postgresql.table,function(e,d){
                            should.not.exist(e)
                            should.exist(d)
                            d.should.have.property('rows').with.lengthOf (1210 + 50)
                            //console.log(d.rows.length)
                            d.rows.forEach(function(row,i){
                                row.should.have.keys(
                                    'id'
                                    ,'ts'
                                    ,'radar_lane_id'
                                    ,'station_lane_id'
                                    ,'name'
                                    ,'route'
                                    ,'direction'
                                    ,'postmile'
                                    ,'enabled'
                                    ,'firmware'
                                    ,'sample_interval'
                                    ,'lastpolltime'
                                    ,'lastgoodpoll'
                                    ,'speed'
                                    ,'speed_units'
                                )
                            })
                            return callback()
                        })
                    })
                    tasks.push(function(callback){
                        client.query('select * from perlhash',function(e,d){
                            should.not.exist(e)
                            should.exist(d)
                            d.should.have.property('rows').with.lengthOf(0) //because I called truncate
                            //console.log(d.rows.length)

                            return callback()
                        })
                    })

                    tasks.push(function(callback){
                        client.query('select * from bt_xml_project',function(e,d){
                            should.not.exist(e)
                            should.exist(d)
                            d.should.have.property('rows').with.lengthOf(1)
                            return callback()
                        })
                    })
                    tasks.push(function(callback){
                        client.query('select * from bt_xml_location',function(e,d){
                            should.not.exist(e)
                            should.exist(d)
                            d.should.have.property('rows').with.lengthOf(5)
                            return callback()
                        })
                    })
                    tasks.push(function(callback){
                        client.query('select * from bt_xml_segment',function(e,d){
                            should.not.exist(e)
                            should.exist(d)
                            d.should.have.property('rows').with.lengthOf(8)
                            return callback()
                        })
                    })
                    tasks.push(function(callback){
                        client.query('select * from bt_xml_observation',function(e,d){
                            should.not.exist(e)
                            should.exist(d)
                            d.should.have.property('rows').with.lengthOf(1210+50)
                            return callback()
                        })
                    })

                    tasks.forEach(function(t) { q.defer(t); });
                    q.awaitAll(function(error, results) {
                        //console.log("all done with db checks")
                        client_done()
                        pg.end()
                        return done()
                    })


                 })

                // open a reader

                return null

            })


    });
  });


})
