# AWS Redshift DB Writer

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

1. Clone the repository: 
```
git clone git@github.com:keboola/db-writer-redshift.git
```
2. Change directory: 
```
cd db-writer-redshift
```
3. Create `.env` file from `.env.template` and fill in values for environment variables: 
```
cp .env.template .env
```
4. 
   1. Run test suite (phpcs, phpstan, phpunit) via docker-compose: 
    ```
    docker-compose run --rm tests
    ```
   2. Run single phpunit test, the test name in this case is "testWrongColumnOrder":
    ```
    docker-compose run --rm ./vendor/bin/phpunit --filter testWrongColumnOrder  
    ```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
