/* global require console process describe it */

var bt_parser=require('../lib/parser')
var logger = require('./logger')('loadbluetoothdump::process_data')

var path    = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../config.json'

var config_okay = require('config_okay')

var config={}


var pg = require('pg')
var rw = require ('rw')
var os = require('os')

var files = // read env vars here?  parse command line arguments?
        ["test/bluetooth_log-2014-07-28-21\:00\:00.048" //bit file1210 records
         ,"test/bluetoothdump" // small file, 50 records
        ]

var queue = require("queue-async")

var masterq = queue(1);

masterq.defer(function(cb){
    config_okay(config_file,function(err,c){
        // should start using node-postgres/lib/connection-parameters.js here
        // which appears to default to the pg convention of using PGUSER, PGPASS, etc in env vars
        if(err) return cb(err)
        if(!c.postgresql.db){ throw new Error('need valid postgresql.db defined in test.config.json')}
        if(!c.postgresql.table){ throw new Error('need valid postgresql.table defined in test.config.json')}
        if(!c.postgresql.user){ throw new Error('need valid postgresql.username defined in test.config.json')}
        if(!c.postgresql.pass){ throw new Error('need valid postgresql.password defined in test.config.json')}

        // sane defaults
        if(c.postgresql.host === undefined) c.postgresql.host = 'localhost'
        if(c.postgresql.port === undefined) c.postgresql.port = 5432

        config = c


        var connstring = "pg://"
                + c.postgresql.user+":"
                + c.postgresql.pass+"@"
                + c.postgresql.host+":"
                + c.postgresql.port+"/"
                + c.postgresql.db
        config.connstring = connstring

        return cb()
    })
    return null
})

// once the config file is sorted, then I can create a connection and a client

masterq.defer(function(master_cb){
    pg.connect(config.connstring, function (err, client, client_done) {
        var fq = new queue(1)
        fq.defer(function(cb2){
            return bt_parser.set_search_path(client,cb2)
        })
        fq.defer(function(cb2){
            var create_statement = bt_parser.create_perlhash_statement('perlhash')
            client.query(create_statement,function(e,r){
                //console.log(e)
                return cb2(e)
            })
        })
        files.forEach(function(f){
            fq.defer(process_file,client,f)
            return null
        })
        fq.awaitAll(function(error) {
            if(error !== undefined){
                logger.info('error parsing files')
                logger.error(error)
            }else{
                logger.info('done parsing files')
            }
            client_done()
            pg.end()
            return master_cb(error)
        })
        return null
    })
})
masterq.awaitAll(function(error) {
    logger.info('closing')
    return null
})


function process_file(client, file,done_file_callback){
    logger.info('processing ',file)
    var _reader = rw.fileReader(file)
    var parser_instance

    var copy_statement = bt_parser.copy_statement(config.postgresql.table)
    var writer = client.copyFrom( copy_statement );

    writer.on('error', function (error) {
        console.log("Sorry, error happens", error);
        throw new Error("COPY FROM stream should not emit errors" + JSON.stringify(error))
    });

    writer.on('close',function(error){
        //console.log("Data inserted sucessfully");
        logger.info('processing xml data from ',file)

        var copy_statement_perl = bt_parser.copy_perlhash_statement('perlhash')
        var perlwriter = client.copyFrom( copy_statement_perl );
        parser_instance.perl_write(perlwriter)
        perlwriter.on('close',function(err){
            logger.info('done copying xml data to db')
            parser_instance.perl_parser(client,function(e){
                logger.info('done pocessing xml data')
                parser_instance.perl_truncate(client,done_file_callback)
                return null
            })
        })
    })
    parser_instance=bt_parser(_reader,writer)
    return null
}
