var should = require('should')

var pg = require('pg')

var temp_table_query="create temp table names(user_name varchar(128),age integer)"

var prepare_table = function (client, callback) {
    client.query(temp_table_query
                ,function (err, result) {
                     should.not.exist(err)
                     callback(err,result);
                 })
}

var M = 10000

pg.connect({'user':'postgres',database:'test'},function (err, client, client_done) {
    prepare_table(client,function(e,r){
        if (e) throw new Error('die')
        // temp table created, test copying into it
        var writer = client.copyFrom("COPY names (user_name, age) FROM STDIN WITH CSV");

        var i = M


        writer.on('error', function (error) {
            console.log("croak: ", error);
            throw new Error('croak')
        })
        writer.on('drain',function(){
            console.log('drained'+i)
            if(i>0)write()
            return null
        })

        function write(){
            var ok = true;
            do{
                i--
                ok = writer.write('user,'+i+'\n')

            }while(i>0
                   //&& ok
                  )
            if(i>0){
                // write is clogged
                console.log('wait for drain')
                // probably a race condition, but this triggers just once
                // return writer.once('drain',write)
                return null
            }
            // done writing
            writer.end()
            // check db here
            console.log('inspect write output')
            client.query('select * from names',function(e,d){
                console.log('query done')
                should.not.exist(e)
                should.exist(d)
                d.should.have.property('rows').with.lengthOf(M
)
                // done
                client_done()
                pg.end()
                return null
            })
            return null
        }
        return write()

    })
})
