# About
This is a web server that takes json data formatted as collectd JSON and puts it into a Postgres database.

# Usage
Once installed, you'll want to edit your `gdata2pg.ini.default` file (and rename it) to set it up as you desire.  It's a fairly straightforward configuration.  Be sure to set your users up in the `[users]` section of the ini file.  This will be used for the basic auth for your server.

You'll also need to install Postgres and get that configured in a basic sense (out of the scope of this README).  You'll need to create a database and user for the db connections.  The included `tsd.sql` file can then be used to create the tables in your db.

```
psql dbname < tsd.sql
```

Once this is all setup, just run the `server.py -c path/to/config` and you should be up and running.  Now, you can just configure the `write_http` module in your `collectd.conf` to point at your server and data should start getting recorded.  Optionally, you could also [taxman](https://github.com/crustymonkey/taxman) to create plugins and submit custom data.

# rollups.py
This is an included script to perform data rollups in Postgres.  You can define at what age your data is rolled up and to what aggregate.  This is also configured in the `gdata2pg.ini` file and uses the same Postgres information for the db connection.  The best thing to do is set this up to run as cron/systemd.timer.
