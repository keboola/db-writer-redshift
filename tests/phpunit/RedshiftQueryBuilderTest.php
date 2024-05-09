<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Redshift\Tests;

use Keboola\DbWriter\Writer\RedshiftConnection;
use Keboola\DbWriter\Writer\RedshiftConnectionFactory;
use Keboola\DbWriter\Writer\RedshiftQueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class RedshiftQueryBuilderTest extends TestCase
{
    private RedshiftConnection $connection;
    private RedshiftQueryBuilder $queryBuilder;
    private ExportConfig $exportConfig;
    private string $stageTableName = 'stageTableName';

    protected function setUp(): void
    {
        $this->queryBuilder = new RedshiftQueryBuilder();
        $connectionFactory = new RedshiftConnectionFactory();
        $this->exportConfig = $this->createExportConfig();
        $this->connection = $connectionFactory->create(
            $this->exportConfig->getDatabaseConfig(),
            new TestLogger(),
        );
    }

    public function testWriteDataFromS3QueryStatement(): void
    {
        $s3info = [
            'bucket' => 'testBucket',
            'key' => 'testKey',
            'credentials' => [
                'access_key_id' => 'testAccessKeyId',
                'secret_access_key' => 'testSecretAccessKey',
                'session_token' => 'testSessionToken',
            ],
            'region' => 'testRegion',
            'isSliced' => true,
        ];

        $expected = 'COPY "testTable" FROM \'s3://testBucket/testKey\' CREDENTIALS \'aws_access_key_id=testAccessKeyId;aws_secret_access_key=testSecretAccessKey;token=testSessionToken\' REGION AS \'testRegion\' DELIMITER \',\' CSV QUOTE \'"\' NULL AS \'NULL\' ACCEPTANYDATE TRUNCATECOLUMNS MANIFEST GZIP;';

        $this->assertEquals($expected, $this->queryBuilder->writeDataFromS3QueryStatement('testTable', $s3info));
    }

    public function testCreateQueryStatement(): void
    {
        $expected = 'CREATE TEMPORARY TABLE "testTable" ';
        $expected .= '("name" VARCHAR(255) NOT NULL ,"int" int NOT NULL ,';
        $expected .= '"float" FLOAT NOT NULL ,"date" DATE NOT NULL ,PRIMARY KEY ("id"))';

        $this->assertEquals(
            $expected,
            $this->queryBuilder->createQueryStatement(
                $this->connection,
                'testTable',
                true,
                $this->exportConfig->getItems(),
                ['id'],
            ),
        );
    }

    public function testUpsertUpdateRowsQueryStatement(): void
    {
        $expected = 'UPDATE "tableName" SET "name" = "stageTableName"."name",';
        $expected .= '"int" = "stageTableName"."int","float" = "stageTableName"."float",';
        $expected .= '"date" = "stageTableName"."date" FROM "stageTableName"';
        $expected .= ' WHERE "tableName"."id" = "stageTableName"."id"';

        $this->assertEquals(
            $expected,
            $this->queryBuilder->upsertUpdateRowsQueryStatement(
                $this->connection,
                $this->exportConfig,
                $this->stageTableName,
            ),
        );
    }

    public function testUpsertDeleteRowsQueryStatement(): void
    {
        $expected = 'DELETE FROM "stageTableName" USING "tableName" WHERE "tableName"."id" = "stageTableName"."id"';

        $this->assertEquals(
            $expected,
            $this->queryBuilder->upsertDeleteRowsQueryStatement(
                $this->connection,
                $this->exportConfig,
                $this->stageTableName,
            ),
        );
    }

    public function testUpsertQueryStatement(): void
    {
        $expected = 'INSERT INTO "tableName" ("name", "int", "float", "date") SELECT * FROM "stageTableName"';

        $this->assertEquals(
            $expected,
            $this->queryBuilder->upsertQueryStatement(
                $this->connection,
                $this->exportConfig,
                $this->stageTableName,
            ),
        );
    }

    private function createExportConfig(): ExportConfig
    {
        return ExportConfig::fromArray(
            [
                'data_dir' => 'dbName',
                'writer_class' => 'tableName',
                'tableId' => 'tableId',
                'dbName' => 'tableName',
                'items' => [
                    [
                        'name' => 'name',
                        'dbName' => 'name',
                        'type' => 'VARCHAR',
                        'size' => '255',
                        'nullable' => false,
                        'default' => null,
                    ],
                    [
                        'name' => 'int',
                        'dbName' => 'int',
                        'type' => 'int',
                        'size' => null,
                        'nullable' => false,
                        'default' => null,
                    ],
                    [
                        'name' => 'float',
                        'dbName' => 'float',
                        'type' => 'FLOAT',
                        'size' => null,
                        'nullable' => false,
                        'default' => null,
                    ],
                    [
                        'name' => 'date',
                        'dbName' => 'date',
                        'type' => 'DATE',
                        'size' => null,
                        'nullable' => false,
                        'default' => null,
                    ],
                ],
                'primaryKey' => ['id'],
                'incremental' => false,
                'export' => false,
                'db' => [
                    'host' => (string) getenv('DB_HOST'),
                    'port' => (string) getenv('DB_PORT'),
                    'user' => (string) getenv('DB_USER'),
                    '#password' => (string) getenv('DB_PASSWORD'),
                    'database' => (string) getenv('DB_DATABASE'),
                    'schema' => (string) getenv('DB_SCHEMA'),
                ],
            ],
            [
                [
                    'source' => 'tableId',
                    'destination' => 'dbName',
                    'columns' => ['name', 'int', 'float', 'date'],
                ],
            ],
        );
    }
}
