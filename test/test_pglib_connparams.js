var should = require('should')

var pg = require('pg')

var Client = pg.Client;

var expected_user = process.env['PGUSER']
var expected_db   = process.env['PGDATABASE']

should.exist(expected_user,'please set env var PGUSER')
should.exist(expected_db,'please set env var PGDATABASE')

describe('obey environment variables',function(){
    it('should work with client',function(done){
        var client = new Client()
        should.exist(client)
        client.should.have.property('user',expected_user)
        client.should.have.property('database',expected_db)
        return done()
    })

    it('should work with pg.connect',function(done){
        pg.connect(function(e,client,client_done){
            should.exist(client)
            client.should.have.property('user',expected_user)
            client.should.have.property('database',expected_db)
            client_done()
            pg.end()
            return done()
        })
    })
})
