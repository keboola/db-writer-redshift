<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 12/02/16
 * Time: 16:38
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;

class Redshift extends Writer implements WriterInterface
{
    private static $allowedTypes = [
        'int', 'int2', 'int4', 'int8',
        'smallint', 'integer', 'bigint',
        'decimal', 'real', 'double precision', 'numeric',
        'float', 'float4', 'float8',
        'boolean',
        'char', 'character', 'nchar', 'bpchar',
        'varchar', 'character varying', 'nvarchar', 'text',
        'date', 'timestamp', 'timestamp without timezone'
    ];

    /** @var \PDO */
    protected $db;

    /** @var Logger */
    protected $logger;

    protected $dbParams;

    public function __construct($dbParams, Logger $logger)
    {
        parent::__construct($dbParams, $logger);
        $this->logger = $logger;
        $this->dbParams = $dbParams;
    }

    public function createConnection($dbParams)
    {
        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
        ];

        // check params
        foreach (['host', 'database', 'user', 'password', 'schema'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '5439';
        $dsn = "pgsql:host={$dbParams['host']};port={$port};dbname={$dbParams["database"]}";

        $this->logger->info(
            "Connecting to DSN '" . $dsn . "'...",
            [
                'options' => $options
            ]
        );

        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['password'], $options);
        $pdo->exec("SET search_path TO \"{$dbParams["schema"]}\";");

        return $pdo;
    }

    public function writeFromS3($s3info, array $table)
    {
        $s3key = $s3info["bucket"] . "/" . $s3info["key"];

        if (isset($s3info["isSliced"]) && $s3info["isSliced"] === true) {
            // empty manifest handling
            $manifest = $this->downloadManifest($s3info, "s3://{$s3key}");

            if (!count($manifest['entries'])) {
                return;
            }
        }

        // Generate copy command
        $command = "COPY \"{$table['dbName']}\" FROM 's3://{$s3key}'"
            . " CREDENTIALS 'aws_access_key_id={$s3info["credentials"]["access_key_id"]};aws_secret_access_key={$s3info["credentials"]["secret_access_key"]};token={$s3info["credentials"]["session_token"]}'"
            . " REGION AS '{$s3info["region"]}' DELIMITER ',' CSV QUOTE '\"'"
            . " NULL AS 'NULL' ACCEPTANYDATE TRUNCATECOLUMNS";

        // Sliced files use manifest and no header
        if (isset($s3info["isSliced"]) && $s3info["isSliced"] === true) {
            $command .= " MANIFEST";
        } else {
            $command .= " IGNOREHEADER 1";
        }

        $command .= " GZIP;";

        $this->logger->info('Executing COPY command');

        try {
            $this->execQuery($command);
        } catch (\PDOException $e) {
            $this->logger->err(sprintf('Write failed: %s', $e->getMessage()), [
                'exception' => $e
            ]);

            $query = $this->db->query("SELECT * FROM stl_load_errors WHERE query = pg_last_query_id();");
            $result = $query->fetchAll();
            $params = [
                "query" => $command
            ];
            if (count($result)) {
                $message = "Table '{$table['dbName']}', column '" . trim($result[0]["colname"]) . "', line {$result[0]["line_number"]}: ". trim($result[0]["err_reason"]);
                $params["redshift_errors"] = $result;
            } else {
                $message = "Query failed: " . $e->getMessage();
            }
            throw new UserException($message, 0, $e, $params);
        } catch (\Exception $e) {
            $this->logger->err(sprintf('Write failed: %s', $e->getMessage()), [
                'exception' => $e
            ]);
        }
    }

    public function isTableValid(array $table, $ignoreExport = false)
    {
        // TODO: Implement isTableValid() method.

        return true;
    }

    public function drop($tableName)
    {
        $this->execQuery(sprintf("DROP TABLE IF EXISTS %s;", $this->escape($tableName)));
    }

    public function create(array $table, $options = [])
    {
        $sql = sprintf(
            "CREATE %s TABLE %s (",
            isset($options['temporary']) && $options['temporary'] ? 'TEMPORARY' : '',
            $this->escape($table['dbName'])
        );

        $columns = array_filter($table['items'], function ($item) {
            return (strtolower($item['type']) !== 'ignore');
        });
        foreach ($columns as $col) {
            $type = strtoupper($col['type']);
            if (!empty($col['size'])) {
                $type .= "({$col['size']})";
            }
            $null = $col['nullable'] ? 'NULL' : 'NOT NULL';
            $default = empty($col['default']) ? '' : "DEFAULT '{$col['default']}'";
            if ($type == 'TEXT') {
                $default = '';
            }
            $sql .= "{$this->escape($col['dbName'])} $type $null $default";
            $sql .= ',';
        }
        $sql = substr($sql, 0, -1);
        $sql .= ");";

        $this->execQuery($sql);
    }

    public function upsert(array $table, $targetTable)
    {
        $sourceTable = $this->escape($table['dbName']);
        $targetTable = $this->escape($targetTable);

        $columns = array_map(
            function ($item) {
                return $this->escape($item['dbName']);
            },
            array_filter($table['items'], function ($item) {
                return strtolower($item['type']) != 'ignore';
            })
        );

        if (!empty($table['primaryKey'])) {
            // update data
            $joinClauseArr = [];
            foreach ($table['primaryKey'] as $index => $value) {
                $joinClauseArr[] = "{$targetTable}.{$value}={$sourceTable}.{$value}";
            }
            $joinClause = implode(' AND ', $joinClauseArr);

            $valuesClauseArr = [];
            foreach ($columns as $index => $column) {
                $valuesClauseArr[] = "{$column}={$sourceTable}.{$column}";
            }
            $valuesClause = implode(',', $valuesClauseArr);

            $query = "
                UPDATE {$targetTable}
                SET {$valuesClause}
                FROM {$sourceTable}
                WHERE {$joinClause}
            ";

            $this->execQuery($query);

            // delete updated from temp table
            $query = "
                DELETE FROM {$sourceTable}
                USING {$targetTable}
                WHERE {$joinClause}
            ";

            $this->execQuery($query);
        }

        // insert new data
        $columnsClause = implode(',', $columns);
        $query = "INSERT INTO {$targetTable} ({$columnsClause}) SELECT * FROM {$sourceTable}";
        $this->execQuery($query);

        // drop temp table
        $this->drop($table['dbName']);
    }

    public static function getAllowedTypes()
    {
        return self::$allowedTypes;
    }

    public function tableExists($tableName)
    {
        $stmt = $this->db->query(sprintf("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'", $tableName));
        $res = $stmt->fetchAll();
        return !empty($res);
    }

    /**
     * @param $query
     * @return int|null
     * @throws UserException
     */
    private function execQuery($query)
    {
        // remove credentials
        $queryToLog = preg_replace(
            '/aws_access_key_id=.*;aws_secret_access_key=.*/',
            'aws_access_key_id=***;aws_secret_access_key=***',
            $query
        );

        $this->logger->info(sprintf("Executing query: '%s'", $queryToLog));

        $tries = 0;
        $maxTries = 3;
        $exception = null;

        while ($tries < $maxTries) {
            $exception = null;
            try {
                return $this->db->exec($query);
            } catch (\PDOException $e) {
                $exception = $this->handleDbError($e, $queryToLog);
                $this->logger->info(sprintf('%s. Retrying... [%dx]', $exception->getMessage(), $tries + 1));
            } catch (\ErrorException $e) {
                $exception = $this->handleDbError($e, $queryToLog);
                $this->logger->info(sprintf('%s. Retrying... [%dx]', $exception->getMessage(), $tries + 1));
            }
            sleep(pow($tries, 2));
            $this->db = $this->createConnection($this->dbParams);
            $tries++;
        }

        if ($exception) {
            throw $exception;
        }

        return null;
    }

    private function handleDbError(\Exception $e, $query = '')
    {
        $message = sprintf('DB query failed: %s', $e->getMessage());
        $exception = new UserException($message, 0, $e, ['query' => $query]);

        try {
            $this->db = $this->createConnection($this->dbParams);
        } catch (\Exception $e) {
        };
        return $exception;
    }

    public function showTables($dbName)
    {
        throw new ApplicationException("Method not implemented");
    }

    public function getTableInfo($tableName)
    {
        throw new ApplicationException("Method not implemented");
    }

    public function write(CsvFile $csv, array $table)
    {
        throw new ApplicationException("Method not implemented");
    }

    private function escape($str)
    {
        return '"' . $str . '"';
    }

    public function testConnection()
    {
        $this->db->query('select current_date')->execute();
    }

    private function downloadManifest($s3Info, $path)
    {
        $s3Client = new \Aws\S3\S3Client([
            'credentials' => [
                'key' => $s3Info['credentials']['access_key_id'],
                'secret' => $s3Info['credentials']['secret_access_key'],
                'token' =>  $s3Info['credentials']['session_token']
            ],
            'region' => $s3Info['region'],
            'version' => '2006-03-01',
        ]);
        $path = parse_url($path);
        $response = $s3Client->getObject([
            'Bucket' => $path['host'],
            'Key' => ltrim($path['path'], '/'),
        ]);
        return json_decode((string)$response['Body'], true);
    }

    public function generateTmpName($tableName)
    {
        return $tableName . '_temp_' . uniqid();
    }
}
