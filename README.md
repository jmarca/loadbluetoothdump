# loadbluetoothdump

if you use this and you're not james marca, you are insane.

# waht?

This library parses a bunch of data from bluetooth detector dump from
an oracle database, so as to load them into my postgresql database.

It uses node-postgres and rw

typical command line:

```
node lib/process_data.js /home/ftp/incoming/bluetooth_log-2014-07-30-*
```

but first you had better set up config.json properly, something like

```
{
    "postgresql": {
        "host": "127.0.0.1",
        "port":5432,
        "db": "mydb",
        "table":"bluetooth_data",
        "schema":"smartsig",
        "user":"myuser",
        "pass":"mypass"
    }
}
```

and then make sure to create the db stuff by doing


```
psql -U myuser -f sql/create.schema.sql mydb
psql -U myuser -f sql/perl_hash_decode.sql mydb
```

This will properly create tables and functions that are needed.

Then you can run the command line to parse files.

```
node lib/process_data.js /home/ftp/incoming/bluetooth_log-2014-07-30-*
```

It won't re-process a file, as it uses copy statements.  So if you
break something or some parse is broken because a file is broken, then
you will need to either truncate the existing dbs and start over, or
else split the file in question to get rid of the lines already
processed.

Finally, this *only* understands the bluetooth data from the Beach
Blvd (SR39) reports.  Other detectors were not examined or processed.
