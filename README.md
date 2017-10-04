# AWS Redshift DB Writer

[![Docker Repository on Quay](https://quay.io/repository/keboola/db-writer-redshift/status "Docker Repository on Quay")](https://quay.io/repository/keboola/db-writer-redshift)
[![Build Status](https://travis-ci.org/keboola/db-writer-redshift.svg?branch=master)](https://travis-ci.org/keboola/db-writer-redshift)
[![Code Climate](https://codeclimate.com/github/keboola/db-writer-redshift/badges/gpa.svg)](https://codeclimate.com/github/keboola/db-writer-redshift)
[![Test Coverage](https://codeclimate.com/github/keboola/db-writer-redshift/badges/coverage.svg)](https://codeclimate.com/github/keboola/db-writer-redshift/coverage)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/keboola/db-writer-redshift/blob/master/LICENSE.md)

Writes data to Redshift Database.

## Example configuration

```json
    {
      "db": {        
        "host": "HOST",
        "port": "PORT",
        "database": "DATABASE",
        "user": "USERNAME",
        "password": "PASSWORD",
        "schema": "SCHEMA"
        "ssh": {
          "enabled": true,
          "keys": {
            "private": "ENCRYPTED_PRIVATE_SSH_KEY",
            "public": "PUBLIC_SSH_KEY"
          },
          "sshHost": "PROXY_HOSTNAME"
        }
      },
      "tables": [
        {
          "tableId": "simple",
          "dbName": "simple",
          "export": true, 
          "incremental": true,
          "primaryKey": ["id"],
          "items": [
            {
              "name": "id",
              "dbName": "id",
              "type": "int",
              "size": null,
              "nullable": null,
              "default": null
            },
            {
              "name": "name",
              "dbName": "name",
              "type": "nvarchar",
              "size": 255,
              "nullable": null,
              "default": null
            },
            {
              "name": "glasses",
              "dbName": "glasses",
              "type": "nvarchar",
              "size": 255,
              "nullable": null,
              "default": null
            }
          ]                                
        }
      ]
    }
```

## Development

App is developed on localhost using TDD.

1. Clone from repository: `git clone git@github.com:keboola/db-writer-redshift.git`
2. Change directory: `cd db-writer-redshift`
3. Create `.env` file with variables:
```
STORAGE_API_TOKEN=
REDSHIFT_DB_HOST=
REDSHIFT_DB_PORT=5439
REDSHIFT_DB_USER=
REDSHIFT_DB_PASSWORD=
REDSHIFT_DB_DATABASE=
REDSHIFT_DB_SCHEMA=
```
4. Run docker-compose, which will trigger phpunit: `docker-compose run --rm tests`
