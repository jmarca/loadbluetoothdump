/*global process console */
var os = require('os')
var rw = require ('rw')
var unescape = require('querystring').unescape
var logger = require('./logger')('loadbluetoothdump::parser')
var queue = require("queue-async")

function copy_statement(table){
    return  ["COPY "
            ,table
            ,"("
            ,bt_parser.copy_columns()
            ,")"
            ,"FROM STDIN WITH CSV "// FORMAT csv "
            ].join(' ')
}

function create_perlhash_statement(table,fk){
    // in production this should be create temp table.
    var create= "CREATE TABLE "+table+" (data text,ts timestamp with time zone not null, radar_lane_id integer not null, station_lane_id integer not null,"
    var constraint
    if(fk){
        constraint = " foreign key (ts,radar_lane_id,station_lane_id) references bluetooth_data(ts,radar_lane_id,station_lane_id))"
    }else{
        constraint = " unique(ts,radar_lane_id,station_lane_id))"
    }
    return create + constraint
}
function copy_perlhash_statement(table){
    return "COPY "+table+" (data,ts,radar_lane_id,station_lane_id) FROM STDIN WITH CSV "
}

function copy_columns(){
    var copy_statement = // ,'id,' // id is autogemerated
    ['radar_lane_id'
    ,'station_lane_id'
    ,'name'
    ,'route'
    ,'direction'
    ,'postmile'
    ,'enabled'
    ,'firmware'
    ,'sample_interval'
    ,'speed'
    ,'speed_units'
    ,'ts'
    ,'lastpolltime'
    ,'lastgoodpoll'
    ].join(', ')
    return copy_statement
}

function hash_stringify(r){
    var string_entries = [r.xml,r.ts].join(',')
    var int_entries = [r.radar_lane_id,r.station_lane_id].join(',')
    var csv = [string_entries,int_entries].join(',')
    return csv
}
function stringify(row){
    // make row into a csv, in the expected order
    //console.log(Object.keys(row))
    var regular_entries=[row.radar_lane_id,
                         row.station_lane_id,
                         row.name,
                         row.route,
                         row.direction,
                         row.postmile,
                         row.enabled,
                         row.firmware,
                         row.sample_interval,
                         row.speed,
                         row.speed_units
                        ].join(',')
    // time is separate because I was having issues and thought I
    // might have to escape or quote the entries.  turns out I do not
    var time_entries = [row.timestamp
                       ,row.lastpolltime
                       ,row.lastgoodpoll
                       ].join(',')
    var csv = [regular_entries,time_entries].join(',')
    return csv
}

//var _writer = rw.fileWriter("../test/dumpout.csv")
//var _reader = rw.fileReader("../test/bluetoothdump")
var parser = rw.dsvParser()
// var perlhash=[]
// var perl_write=function(perlwriter){
//     perlwriter.write(perlhash.join('\n'))
//     return perlwriter.end()
// }

function set_search_path(client,callback){
    client.query("SET search_path TO smartsig,public",callback)
}
function bt_parser(reader,writer){

    // if(!reader) reader = _reader
    // if(!writer) writer = _writer
    var perlhash=[]
    this.perl_write=function(perlwriter){
        perlhash.forEach(function(r){
            perlwriter.write(hash_stringify(r)+'\n')
            return null
        })
        return perlwriter.end()
    }
    this.perl_parser=function(client,callback){
        // essentially, I have to do these in order:

        var insert_statements = []
        insert_statements.push([
            "with"
            ,"a as ("
            ,"  select distinct * from perl_xml_project_decoder_from_location()"
            ,"),"
            ,"b as ("
            ,"  select a.*"
            ,"  from a"
            ,"  left outer join bt_xml_project z USING (projectid)"
            ,"  where z.projectid is null"
            ,")"
            ,"insert into bt_xml_project (projectid,title) (select projectid,title from b)"
        ].join(' '))

        insert_statements.push(
            ["with a as ("
             ,"select aa.*,count(*) as cnt from perl_xml_location_decoder_from_location() aa"
             ,"left outer join bt_xml_location z USING(locationid)"
             ,"where z.locationid is null"
             ,"group by aa.locationid,aa.locationname,aa.latitude,aa.longitude,aa.projectid"
             ,"),"
             ,"b as ("
             ,"select locationid,locationname,latitude,longitude,projectid,"
             ,"rank() OVER (PARTITION BY locationid ORDER BY cnt DESC) AS pos"
             ,"from a"
             ,")"
             ,"insert into bt_xml_location (locationid,locationname,latitude,longitude,projectid)"
             ,"(select locationid,locationname,latitude,longitude,projectid"
             ,"from b"
             ,"where pos=1)"].join(' ')
            )
        insert_statements.push(
            'insert into bt_xml_segment  (select distinct * from perl_xml_segment_decoder())'
            ,'insert into bt_xml_observation  (select  * from perl_xml_segment_obs_decoder())'
        )


        var q = queue(1);

        insert_statements.forEach(function(statement) {
            q.defer(function(cb){
                client.query(statement
                             ,function (err, result) {
                                 console.log(statement)
                                 console.log(err)
                                 return cb(err)
                             })
            })
            return null
        })
        q.awaitAll(function(error, results) {
            console.log("all done with insert statements")
            return callback()
        })

    }

    var rows=[]
    //var count = 0
    logger.debug('writing records')
    function writeout(){
        var row = rows.shift() || parser.pop(reader.ended)
        var ok=true
        if(row != null){
            do{
                //count++
                if(row.route === 'SR-39'){
                    perlhash.push({xml:row.xml
                                   ,ts:row.timestamp
                                   ,radar_lane_id:  row.radar_lane_id
                                   ,station_lane_id:row.station_lane_id
                         })

                    delete row.xml

                    // make a new csv string
                    var csv = stringify(row)
                    //console.log(csv)
                    ok = // write always returns false
                        writer.write(csv+'\n')
                }
                row=parser.pop(reader.ended)
            } while (row != null )// && ok)
        }
        if(row){
            // have to wait for drain
            rows.unshift(row)
        }
        if (reader.ended){
            //console.log('row count = ',count)
            return writer.end()
        }
        // annoying, but drain is never fired by pg writer

        //if(ok)
         return reader.fill(flow);

        //console.log('wait for drain event')
        // writer.once('drain',writeout)
        return writeout()
        //return null
    }

    writer.on('drain',function(){
        logger.debug('drain')
        return null
    })

    var data
    function flow(err){
        logger.debug('flow')
        if (err) throw err;
        data=reader.read()
        var row;
        if(data) parser.push(data);
        return writeout()
    }

    reader.fill(flow)
    return this
}

var xmllocations={}
var xmlsegments={}
var xmlnodes={}


bt_parser.copy_columns=copy_columns
bt_parser.copy_statement=copy_statement
bt_parser.create_perlhash_statement=create_perlhash_statement
bt_parser.copy_perlhash_statement=copy_perlhash_statement
bt_parser.set_search_path=set_search_path

module.exports = bt_parser
//bt_parser()
