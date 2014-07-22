var should = require('should')
var config={}


var pg = require('pg')
var Client = pg.Client
var client = new Client({user: 'slash', database: 'test'});
client.connect();
client.query("drop table names")
client.query("create table names(user_name varchar(128),age integer)")
var writer = client.copyFrom("COPY names (user_name, age) FROM STDIN WITH CSV");
writer.on('close', function () {
    client.end()
});
writer.on('error', function (error) {
    console.log("Sorry, error happens", error);
});
var rows = [
    "user1,10"
  ,"user2,20"
  ,"user3,30"
  ,"user4,40"

]


function write(){
    var ok = true;
    var row = rows.shift()
    if(row !== undefined){
        do {
            //console.log('writing',row)
            ok=writer.write(row+'\n')
            row=rows.shift()
        } while (row!==undefined && ok);
    }
    if (row) {
        // had to stop early!
        // write some more once it drains
        //writer.once('drain', write);
        rows.unshift(row)
    }else{
        //console.log('done')
        //writer.removeAllListeners('drain')
        writer.end()
    }
}

var h = writer.on('drain',function(e){
    //console.log('rows left')
    //console.log(rows)
    console.log('drain called')
    write()
})

write()
