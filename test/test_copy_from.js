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

var temp_table_query
var prepare_table = function (client, callback) {
    client.query(temp_table_query
                ,function (err, result) {
                     should.not.exist(err)
                     callback(err,result);
                 })
}

//var bt_parser = require('../.')
var bt_parser=require('../lib/parser2')

before(function(done){
    config_okay(config_file,function(err,c){

        // should start using node-postgres/lib/connection-parameters.js here
        // which appears to default to the pg convention of using PGUSER, PGPASS, etc in env vars

        if(!c.postgresql.db){ throw new Error('need valid postgresql.db defined in test.config.json')}
        if(!c.postgresql.table){ throw new Error('need valid postgresql.table defined in test.config.json')}
        if(!c.postgresql.user){ throw new Error('need valid postgresql.username defined in test.config.json')}
        if(!c.postgresql.pass){ throw new Error('need valid postgresql.password defined in test.config.json')}

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
          ,'speed_units varchar(32))'
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

    it('should work',function(done){
        pg.connect(config.connstring, function (err, client, conn_done) {
            should.not.exist(err)
            var lines=0
            prepare_table(client, function () {
                var copy_statement = bt_parser.copy_statement(config.postgresql.table)
                //console.log(copy_statement)
                var writer = client.copyFrom( copy_statement );

                writer.on('error', function (error) {
                    console.log("Sorry, error happens", error);
                    throw new Error("COPY FROM stream should not emit errors" + JSON.stringify(error))
                });

                writer.on('close',function(error){
                    //console.log("Data inserted sucessfully");
                    should.not.exist(error)
                    // check db here
                    client.query('select * from '+config.postgresql.table,function(e,d){
                        should.not.exist(e)
                        should.exist(d)
                        d.should.have.property('rows').with.lengthOf(50)
                        client.end()
                        return done()
                    })
                })


                // open a reader

                var _reader = rw.fileReader("test/bluetoothdump")
                bt_parser(_reader,writer)
                return null

            })


    });
  });

})
