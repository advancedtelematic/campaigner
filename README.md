# ATS campaigner

## Background

Schedule campaigns in batches for devices within groups.

## Building

Depends on:
```
- device-registry
- director
```

## Testing

You'll need a mariadb instance running with the users configured in `application.conf`. If you want it quick you can use `deploy/ci_setup.sh`. This will create a new docker container running a database with the proper permissions.

To run tests simply run `sbt test`.

## Deploying

See [service.json](deploy/service.json) and [arch.mmd](docs/arch.mmd) for environment variables and dependencies, respectively.

## Usage

The API is straightforward:

[Campaigner](http://advancedtelematic.github.io/rvi_sota_server/swagger/sota-core.html?url=https://s3.eu-central-1.amazonaws.com/ats-end-to-end-tests/swagger-docs/latest/Campaigner.json)

## License

This code is licensed under the [Mozilla Public License 2.0](LICENSE), a copy of which can be found in this repository. All code is copyright [ATS Advanced Telematic Systems GmbH](https://www.advancedtelematic.com), 2016-2018.
