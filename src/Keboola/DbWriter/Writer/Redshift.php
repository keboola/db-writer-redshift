<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 12/02/16
 * Time: 16:38
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;

class Redshift extends Writer implements WriterInterface
{
    private static $allowedTypes = [
        'int', 'smallint', 'bigint', 'money',
        'decimal', 'real', 'float',
        'date', 'datetime', 'datetime2', 'time', 'timestamp',
        'char', 'varchar', 'text',
        'nchar', 'nvarchar', 'ntext',
        'binary', 'varbinary', 'image',
    ];

    private static $typesWithSize = [
        'identity',
        'decimal', 'float',
        'datetime', 'time',
        'char', 'varchar',
        'nchar', 'nvarchar',
        'binary', 'varbinary',
    ];

    private static $unicodeTypes = [
        'nchar', 'nvarchar', 'ntext',
    ];

    private static $numericTypes = [
        'int', 'smallint', 'bigint', 'money',
        'decimal', 'real', 'float'
    ];

    /** @var \PDO */
    protected $db;

    private $batched = true;

    /** @var Logger */
    protected $logger;

    public function __construct($dbParams, Logger $logger)
    {
        parent::__construct($dbParams, $logger);
        $this->logger = $logger;
    }

    public function createConnection($dbParams)
    {
        if (!empty($dbParams['batched'])) {
            if ($dbParams['batched'] == false) {
                $this->batched = false;
            }
        }

        // check params
        foreach (['host', 'database', 'user', '#password'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $port = isset($dbParams['port']) ? $dbParams['port'] : '1433';

        if ($port == '1433') {
            $dsn = sprintf(
                "dblib:host=%s;dbname=%s;charset=UTF-8",
                $dbParams['host'],
                $dbParams['database']
            );
        } else {
            $dsn = sprintf(
                "dblib:host=%s:%s;dbname=%s;charset=UTF-8",
                $dbParams['host'],
                $port,
                $dbParams['database']
            );
        }

        // mssql dont support options
        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['#password']);
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }

    public function write(CsvFile $csv, array $table)
    {
        $csv->next();
        $csvHeader = $csv->current();
        $items = $this->reorderColumns($csvHeader, $table['items']);
        $csv->next();

        $columnsCount = count($csv->current());
        $rowsPerInsert = intval((1000 / $columnsCount) - 1);

        $this->db->beginTransaction();

        while ($csv->current() !== false) {
            $sql = sprintf(
                    "INSERT INTO %s (%s) ",
                    $this->escape($table['dbName']),
                    implode(',', $csvHeader)
                ) . PHP_EOL;

            for ($i=0; $i<$rowsPerInsert && $csv->current() !== false; $i++) {
                $sql .= sprintf(
                    "SELECT %s UNION ALL" . PHP_EOL,
                    implode(
                        ',',
                        $this->encodeCsvRow(
                            $this->escapeCsvRow($csv->current()),
                            $items
                        )
                    )
                );
                $csv->next();
            }
            // strip the last UNION ALL
            $sql = substr($sql, 0, -10);

            $this->execQuery($sql);
        }

        $this->db->commit();
    }

    private function reorderColumns($csvHeader, $items)
    {
        $reordered = [];
        foreach ($csvHeader as $csvCol) {
            foreach ($items as $item) {
                if ($csvCol == $item['name']) {
                    $reordered[] = $item;
                }
            }
        }

        return $reordered;
    }

    private function encodeCsvRow($row, $columnDefinitions)
    {
        $res = [];
        foreach ($row as $k => $v) {
            if (strtolower($columnDefinitions[$k]['type']) == 'ignore') {
                continue;
            }
            $decider = $this->getEncodingDecider($columnDefinitions[$k]['type']);
            $res[$k] = $decider($v);
        }

        return $res;
    }

    private function getEncodingDecider($type)
    {
        return function ($data) use ($type) {
            if (strtolower($data) === 'null') {
                return $data;
            }

            if (in_array(strtolower($type), self::$numericTypes) && empty($data)) {
                return 0;
            }

            if (!in_array(strtolower($type), self::$numericTypes)) {
                $data = "'" . $data . "'";
            }

            if (in_array(strtolower($type), self::$unicodeTypes)) {
                return "N" . $data;
            }

            return $data;
        };
    }

    private function escapeCsvRow($row)
    {
        $res = [];
        foreach ($row as $k => $v) {
            $res[$k] = $this->msEscapeString($v);
        }

        return $res;
    }

    private function msEscapeString($data)
    {
        if (!isset($data) || empty($data)) {
            return '';
        }
        if (is_numeric($data)) {
            return $data;
        }

        $non_displayables = [
            '/%0[0-8bcef]/',            // url encoded 00-08, 11, 12, 14, 15
            '/%1[0-9a-f]/',             // url encoded 16-31
            '/[\x00-\x08]/',            // 00-08
            '/\x0b/',                   // 11
            '/\x0c/',                   // 12
            '/[\x0e-\x1f]/'             // 14-31
        ];
        foreach ($non_displayables as $regex) {
            $data = preg_replace($regex, '', $data);
        }
        $data = str_replace("'", "''", $data);

        return $data;
    }

    public function isTableValid(array $table, $ignoreExport = false)
    {
        // TODO: Implement isTableValid() method.

        return true;
    }

    public function drop($tableName)
    {
        $this->db->exec(sprintf("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s;", $tableName, $tableName));
    }

    private function escape($obj)
    {
        $objNameArr = explode('.', $obj);

        if (count($objNameArr) > 1) {
            return $objNameArr[0] . ".[" . $objNameArr[1] . "]";
        }

        return "[" . $objNameArr[0] . "]";
    }

    public function create(array $table)
    {
        $sql = "CREATE TABLE `{$table['dbName']}` (";

        $columns = $table['items'];
        foreach ($columns as $k => $col) {
            $type = strtolower($col['type']);
            if ($type == 'ignore') {
                continue;
            }

            if (!empty($col['size']) && in_array($type, self::$typesWithSize)) {
                $type .= "({$col['size']})";
            }

            $sql .= "`{$col['dbName']}` $type";
            $sql .= ',';
        }

        $sql = substr($sql, 0, -1);
        $sql .= ");";

        $this->execQuery($sql);
    }

    public static function getAllowedTypes()
    {
        return self::$allowedTypes;
    }

    public function upsert(array $table, $targetTable)
    {
        $sourceTable = $this->escape($table['dbName']);
        $targetTable = $this->escape($targetTable);

        $columns = array_map(function ($item) {
            if (strtolower($item['type']) != 'ignore') {
                return $this->escape($item['dbName']);
            }
        }, $table['items']);

        if (!empty($table['primaryKey'])) {
            // update data
            $joinClauseArr = [];
            foreach ($table['primaryKey'] as $index => $value) {
                $joinClauseArr[] = "a.{$value}=b.{$value}";
            }
            $joinClause = implode(' AND ', $joinClauseArr);

            $valuesClauseArr = [];
            foreach ($columns as $index => $column) {
                $valuesClauseArr[] = "a.{$column}=b.{$column}";
            }
            $valuesClause = implode(',', $valuesClauseArr);

            $query = "UPDATE a
                SET {$valuesClause}
                FROM {$targetTable} a
                INNER JOIN {$sourceTable} b ON {$joinClause}
            ";

            $this->execQuery($query);

            // delete updated from temp table
            $query = "DELETE a FROM {$sourceTable} a
                INNER JOIN {$targetTable} b ON {$joinClause}
            ";

            $this->execQuery($query);
        }

        // insert new data
        $columnsClause = implode(',', $columns);
        $query = "INSERT INTO {$targetTable} ({$columnsClause}) SELECT * FROM {$sourceTable}";
        $this->execQuery($query);

        // drop temp table
        $this->drop($sourceTable);
    }

    public function tableExists($tableName)
    {
        $tableArr = explode('.', $tableName);
        $tableName = isset($tableArr[1])?$tableArr[1]:$tableArr[0];
        $tableName = str_replace(['[',']'], '', $tableName);
        $stmt = $this->db->query(sprintf("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'", $tableName));
        $res = $stmt->fetchAll();
        return !empty($res);
    }

    private function execQuery($query)
    {
        $this->logger->debug(sprintf("Executing query '%s'", $query));
        $this->db->exec($query);
    }

    public function showTables($dbName)
    {
        // TODO: Implement showTables() method.
    }

    public function getTableInfo($tableName)
    {
        // TODO: Implement getTableInfo() method.
    }
}
