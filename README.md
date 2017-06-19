# ATS campaigner

Schedule campaigns in batches for devices within groups.

Depends on:
```
- device-registry
- director
```

## Running tests

You'll need a mariadb instance running with the users configured in
`application.conf`. If you want it quick you can use
`deploy/ci_setup.sh`. This will create a new docker container running
a database with the proper permissions.

To run tests simply run `sbt test`.

## API

[Campaigner](http://advancedtelematic.github.io/rvi_sota_server/swagger/campaigner.html?url=https://s3.eu-central-1.amazonaws.com/ats-end-to-end-tests/swagger-docs/latest/Campaigner.json)

## Teamcity jobs

In the `deploy` directory there are some scripts you can use to setup
the jobs in Teamcity.
