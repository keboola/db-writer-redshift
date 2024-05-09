<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterAdapter\Query\DefaultQueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;

class RedshiftQueryBuilder extends DefaultQueryBuilder
{
    /**
     * @param array{
     *     bucket: string,
     *     key: string,
     *     credentials: array{
     *          access_key_id: string,
     *          secret_access_key: string,
     *          session_token: string
     *     },
     *     region: string,
     *     isSliced?: bool
     * } $s3info
     */
    public function writeDataFromS3QueryStatement(
        string $tableName,
        array $s3info,
    ): string {
        $s3key = $s3info['bucket'] . '/' . $s3info['key'];
        $command = sprintf('COPY "%s" FROM \'s3://%s\'', $tableName, $s3key);
        $command .= sprintf(
            " CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s;token=%s'",
            $s3info['credentials']['access_key_id'],
            $s3info['credentials']['secret_access_key'],
            $s3info['credentials']['session_token'],
        );
        $command .= sprintf(
            " REGION AS '%s' DELIMITER ',' CSV QUOTE '\"'",
            $s3info['region'],
        );
        $command .= " NULL AS 'NULL' ACCEPTANYDATE TRUNCATECOLUMNS";

        // Sliced files use manifest and no header
        if (isset($s3info['isSliced']) && $s3info['isSliced'] === true) {
            $command .= ' MANIFEST';
        } else {
            $command .= ' IGNOREHEADER 1';
        }
        $command .= ' GZIP;';

        return $command;
    }

    /**
     * @param ItemConfig[] $items
     */
    public function createQueryStatement(
        Connection $connection,
        string $tableName,
        bool $isTempTable,
        array $items,
        ?array $primaryKeys = null,
    ): string {
        $createTable = sprintf(
            'CREATE %s TABLE %s',
            $isTempTable ? 'TEMPORARY' : '',
            $connection->quoteIdentifier($tableName),
        );

        $filteredItems = array_filter(
            $items,
            function (ItemConfig $itemConfig) {
                return strtolower($itemConfig->getType()) !== 'ignore';
            },
        );

        $columnsDefinition = array_map(
            function (ItemConfig $itemConfig) use ($connection) {
                return sprintf(
                    '%s %s%s %s %s',
                    $connection->quoteIdentifier($itemConfig->getDbName()),
                    $itemConfig->getType(),
                    $itemConfig->hasSize() ? sprintf('(%s)', $itemConfig->getSize()) : '',
                    $itemConfig->getNullable() ? 'NULL' : 'NOT NULL',
                    $itemConfig->hasDefault() && $itemConfig->getType() !== 'TEXT' ?
                        'DEFAULT ' . $connection->quote($itemConfig->getDefault()) :
                        '',
                );
            },
            $filteredItems,
        );

        if ($primaryKeys) {
            $columnsDefinition[] = sprintf(
                'PRIMARY KEY (%s)',
                implode(',', array_map(fn($item) => $connection->quoteIdentifier($item), $primaryKeys)),
            );
        }

        return sprintf(
            '%s (%s)',
            $createTable,
            implode(',', $columnsDefinition),
        );
    }

    public function upsertUpdateRowsQueryStatement(
        Connection $connection,
        ExportConfig $exportConfig,
        string $stageTableName,
    ): string {
        $columns = array_map(function ($item) {
            return $item->getDbName();
        }, $exportConfig->getItems());

        // update data
        $whereClauseArr = array_map(fn($item) => sprintf(
            '%s.%s = %s.%s',
            $connection->quoteIdentifier($exportConfig->getDbName()),
            $connection->quoteIdentifier($item),
            $connection->quoteIdentifier($stageTableName),
            $connection->quoteIdentifier($item),
        ), $exportConfig->getPrimaryKey());
        $whereClause = implode(' AND ', $whereClauseArr);

        $valuesClauseArr = array_map(fn($item) => sprintf(
            '%s = %s.%s',
            $connection->quoteIdentifier($item),
            $connection->quoteIdentifier($stageTableName),
            $connection->quoteIdentifier($item),
        ), $columns);
        $valuesClause = implode(',', $valuesClauseArr);

        return sprintf(
            'UPDATE %s SET %s FROM %s WHERE %s',
            $connection->quoteIdentifier($exportConfig->getDbName()),
            $valuesClause,
            $connection->quoteIdentifier($stageTableName),
            $whereClause,
        );
    }

    public function upsertDeleteRowsQueryStatement(
        Connection $connection,
        ExportConfig $exportConfig,
        string $stageTableName,
    ): string {
        $whereClauseArr = array_map(fn($item) => sprintf(
            '%s.%s = %s.%s',
            $connection->quoteIdentifier($exportConfig->getDbName()),
            $connection->quoteIdentifier($item),
            $connection->quoteIdentifier($stageTableName),
            $connection->quoteIdentifier($item),
        ), $exportConfig->getPrimaryKey());
        $whereClause = implode(' AND ', $whereClauseArr);

        return sprintf(
            'DELETE FROM %s USING %s WHERE %s',
            $connection->quoteIdentifier($stageTableName),
            $connection->quoteIdentifier($exportConfig->getDbName()),
            $whereClause,
        );
    }

    public function upsertQueryStatement(
        Connection $connection,
        ExportConfig $exportConfig,
        string $stageTableName,
    ): string {
        $columns = $this->quotedDbColumnNames($exportConfig->getItems(), $connection);

        return sprintf(
            'INSERT INTO %s (%s) SELECT * FROM %s',
            $connection->quoteIdentifier($exportConfig->getDbName()),
            implode(', ', $columns),
            $connection->quoteIdentifier($stageTableName),
        );
    }
}
