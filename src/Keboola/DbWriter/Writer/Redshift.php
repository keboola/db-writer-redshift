<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;

class Redshift extends Writer implements WriterInterface
{
    /** @var array $allowedTypes */
    private static $allowedTypes = [
        'int', 'int2', 'int4', 'int8',
        'smallint', 'integer', 'bigint',
        'decimal', 'real', 'double precision', 'numeric',
        'float', 'float4', 'float8',
        'boolean',
        'char', 'character', 'nchar', 'bpchar',
        'varchar', 'character varying', 'nvarchar', 'text',
        'date', 'timestamp', 'timestamp without timezone',
    ];

    /** @var \PDO $db */
    protected $db;

    /** @var Logger $logger */
    protected $logger;

    /** @var array $dbParams */
    protected $dbParams;

    public function __construct(array $dbParams, Logger $logger)
    {
        parent::__construct($dbParams, $logger);
        $this->logger = $logger;
        $this->dbParams = $dbParams;
    }

    public function createConnection(array $dbParams): \PDO
    {
        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
        ];

        // check params
        foreach (['host', 'database', 'user', 'password', 'schema'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf('Parameter %s is missing.', $r));
            }
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '5439';
        $dsn = sprintf(
            'pgsql:host=%s;port=%s;dbname=%s;keepalives=1;keepalives_idle=60',
            $dbParams['host'],
            $port,
            $dbParams['database']
        );

        $this->logger->info(
            sprintf('Connecting to DSN \'%s\'...', $dsn),
            [
                'options' => $options,
            ]
        );

        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['password'], $options);
        $pdo->exec(sprintf('SET search_path TO "%s";', $dbParams['schema']));

        return $pdo;
    }

    public function writeFromS3(array $s3info, array $table): void
    {
        $s3key = $s3info['bucket'] . '/' . $s3info['key'];

        if (isset($s3info['isSliced']) && $s3info['isSliced'] === true) {
            // empty manifest handling
            $manifest = $this->downloadManifest($s3info, "s3://{$s3key}");

            if (!count($manifest['entries'])) {
                return;
            }
        }

        // Generate copy command
        $command = sprintf('COPY "%s" FROM \'s3://%s\'', $table['dbName'], $s3key);
        $command .= sprintf(
            " CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s;token=%s'",
            $s3info['credentials']['access_key_id'],
            $s3info['credentials']['secret_access_key'],
            $s3info['credentials']['session_token']
        );
        $command .= sprintf(
            " REGION AS '%s' DELIMITER ',' CSV QUOTE '\"'",
            $s3info['region']
        );
        $command .= " NULL AS 'NULL' ACCEPTANYDATE TRUNCATECOLUMNS";

        // Sliced files use manifest and no header
        if (isset($s3info['isSliced']) && $s3info['isSliced'] === true) {
            $command .= ' MANIFEST';
        } else {
            $command .= ' IGNOREHEADER 1';
        }
        $command .= ' GZIP;';

        $this->execQuery($command);
    }

    public function isTableValid(array $table, bool $ignoreExport = false): bool
    {
        // TODO: Implement isTableValid() method.

        return true;
    }

    public function drop(string $tableName): void
    {
        $this->execQuery(sprintf('DROP TABLE IF EXISTS %s;', $this->escape($tableName)));
    }

    public function create(array $table): void
    {
        $sql = sprintf('CREATE TABLE %s (', $this->escape($table['dbName']));

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
            if ($type === 'TEXT') {
                $default = '';
            }
            $sql .= "{$this->escape($col['dbName'])} $type $null $default";
            $sql .= ',';
        }
        $sql = substr($sql, 0, -1);
        $sql .= ');';

        $this->execQuery($sql);
    }

    public function upsert(array $table, string $targetTable): void
    {
        $sourceTable = $this->escape($table['dbName']);
        $targetTable = $this->escape($targetTable);

        $columns = array_map(
            function ($item) {
                return $this->escape($item['dbName']);
            },
            array_filter($table['items'], function ($item) {
                return strtolower($item['type']) !== 'ignore';
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

    public static function getAllowedTypes(): array
    {
        return self::$allowedTypes;
    }

    public function tableExists(string $tableName): bool
    {
        $stmt = $this->db->query(sprintf(
            "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'",
            strtolower($tableName)
        ));
        $res = $stmt->fetchAll();
        return !empty($res);
    }

    private function execQuery(string $query): ?int
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
            } catch (\PDOException | \ErrorException $e) {
                $exception = $this->errorsToException($query);
                $this->logger->error($exception->getMessage(), $exception->getData());
                $this->reconnect();
                $this->logger->info(sprintf('%s. Retrying... [%dx]', $e->getMessage(), $tries + 1));
            }
            sleep(pow($tries, 2));
            $tries++;
        }

        if ($exception) {
            throw $exception;
        }

        return null;
    }

    private function getErrors(): array
    {
        $query = $this->db->query('SELECT * FROM stl_load_errors WHERE query = pg_last_query_id();');
        return $query->fetchAll();
    }

    private function errorsToException(string $query): UserException
    {
        $errors = $this->getErrors();
        $message = '';
        foreach ($errors as $error) {
            $message .= sprintf(
                "Column '%s', line %s: %s",
                trim($error['colname']),
                $error['line_number'],
                trim($error['err_reason'])
            );
        }

        return new UserException($message, 0, null, [
            'query' => $query,
            'redshift_errors' => $errors,
        ]);
    }

    private function reconnect(): void
    {
        try {
            $this->db = $this->createConnection($this->dbParams);
        } catch (\Throwable $e) {
        };
    }

    public function showTables(string $dbName): array
    {
        throw new ApplicationException('Method not implemented');
    }

    public function getTableInfo(string $tableName): array
    {
        throw new ApplicationException('Method not implemented');
    }

    public function write(CsvFile $csv, array $table): void
    {
        throw new ApplicationException('Method not implemented');
    }

    public function validateTable(array $tableConfig): void
    {
        throw new ApplicationException('Method not implemented');
    }


    private function escape(string $str): string
    {
        return '"' . $str . '"';
    }

    public function testConnection(): void
    {
        $this->db->query('select current_date')->execute();
    }

    private function downloadManifest(array $s3Info, string $path): array
    {
        $s3Client = new \Aws\S3\S3Client([
            'credentials' => [
                'key' => $s3Info['credentials']['access_key_id'],
                'secret' => $s3Info['credentials']['secret_access_key'],
                'token' =>  $s3Info['credentials']['session_token'],
            ],
            'region' => $s3Info['region'],
            'version' => '2006-03-01',
        ]);
        $path = parse_url($path);
        $response = $s3Client->getObject([
            'Bucket' => $path['host'],
            'Key' => ltrim($path['path'], '/'),
        ]);
        return json_decode((string) $response['Body'], true);
    }

    public function generateTmpName(string $tableName): string
    {
        return $tableName . '_temp_' . uniqid();
    }
}
