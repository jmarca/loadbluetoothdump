/*global process console */
var os = require('os')
var rw = require ('rw')

function copy_statement(table){
    return  ["COPY "
            ,table
            ,"("
            ,bt_parser.copy_columns()
            ,")"
            ,"FROM STDIN WITH CSV "// FORMAT csv "
            ].join(' ')
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
                         row.speed_units].join(',')
    var time_entries = [//'TIMESTAMP '+
                        row.timestamp
                       ,//'TIMESTAMP '+
                        row.lastpolltime
                       ,//'TIMESTAMP '+
                        row.lastgoodpoll
                       ].join(',')
    var csv = [regular_entries,time_entries].join(',')
    return csv
}

var _writer = rw.fileWriter("../test/dumpout.csv")
var _reader = rw.fileReader("../test/bluetoothdump")
var parser = rw.dsvParser()

function bt_parser(reader,writer){

    if(!reader) reader = _reader
    if(!writer) writer = _writer


    var rows=[]
    //var count = 0
    function writeout(){
        var row = rows.shift() || parser.pop(reader.ended)
        var ok=true
        if(row != null){
            do{
                //count++
                if(row.route === 'SR-39'){
                    delete row.xml
                    // make a new csv string
                    var csv = stringify(row)
                    //console.log(csv)
                    ok = writer.write(csv+'\n')
                }
                row=parser.pop(reader.ended)
            } while (row != null && ok)
        }
        if(row){
            // have to wait for drain
            rows.unshift(row)
        }
        if (reader.ended){
            //console.log('row count = ',count)
            return writer.end()
        }
        if(ok) return reader.fill(flow);
        //console.log('wait for drain event')
        // writer.once('drain',writeout)
        return writeout()
        //return null
    }

    writer.on('drain',function(){
        console.log('drain')
        console.log(rows)
        writeout()
        return null
    })

    var data
    function flow(err){
        if (err) throw err;
        data=reader.read()
        var row;
        if(data) parser.push(data);
        return writeout()
    }

    reader.fill(flow)
    return null
}
bt_parser.copy_columns=copy_columns
bt_parser.copy_statement=copy_statement
module.exports = bt_parser
//bt_parser()
