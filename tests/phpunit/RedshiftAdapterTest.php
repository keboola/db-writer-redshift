<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Redshift\Tests;

use Generator;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Writer\RedshiftAdapter;
use Keboola\DbWriter\Writer\RedshiftConnection;
use Keboola\DbWriter\Writer\RedshiftConnectionFactory;
use Keboola\DbWriter\Writer\RedshiftQueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class RedshiftAdapterTest extends TestCase
{
    private RedshiftConnection $connection;

    private TestLogger $logger;

    public function __construct(?string $name = null, array $data = [], $dataName = '')
    {
        parent::__construct($name, $data, $dataName);

        $this->logger = new TestLogger();

        $connectionFactory = new RedshiftConnectionFactory();
        $this->connection = $connectionFactory->create($this->getDatabaseConfig(), $this->logger);

        if (!$this->getAdapter()->tableExists('testTable')) {
            $this->getAdapter()->create(
                'testTable',
                false,
                [
                    ItemConfig::fromArray([
                        'name' => 'column1',
                        'dbName' => 'column1',
                        'type' => 'VARCHAR',
                    ]),
                    ItemConfig::fromArray([
                        'name' => 'column2',
                        'dbName' => 'column2',
                        'type' => 'VARCHAR',
                    ]),
                ],
            );
        }
    }

    /**
     * @dataProvider tableExistsDataProvider
     */
    public function testTableExists(string $tableName, bool $exist): void
    {
        $this->assertEquals($exist, $this->getAdapter()->tableExists($tableName));
    }

    public function testErrorWriteDataS3NotSet(): void
    {
        $temp = new Temp();
        $temp->createFile('in/tables/testTable.csv.manifest');
        $exportConfig = ExportConfig::fromArray(
            [
                'tableId' => 'testTable',
                'data_dir' => $temp->getTmpFolder(),
                'writer_class' => 'redshift',
                'db' => [
                    'host' => 'testHost',
                    'port' => 'testPort',
                    'database' => 'testDatabase',
                    'user' => 'testUser',
                    '#password' => 'testPassword',
                    'schema' => 'testSchema',
                ],
                'dbName' => 'testDatabase',
                'items' => [],
            ],
            [
                [
                    'source' => 'testTable',
                    'destination' => 'testTable.csv',
                    'columns' => ['column1', 'column2'],
                ],
            ],
        );
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('S3 section not found in manifest');
        $this->getAdapter()->writeData('testTable', $exportConfig);
    }

    public function tableExistsDataProvider(): Generator
    {
        yield 'TableExists' => [
            'testTable',
            true,
        ];

        yield 'TableExists2' => [
            'TeStTaBlE',
            true,
        ];

        yield 'TableNotExists' => [
            'testTableNotExists',
            false,
        ];
    }

    private function getAdapter(): RedshiftAdapter
    {
        return new RedshiftAdapter(
            $this->connection,
            new RedshiftQueryBuilder(),
            $this->logger,
        );
    }

    private function getDatabaseConfig(): DatabaseConfig
    {
        return DatabaseConfig::fromArray([
            'host' => (string) getenv('DB_HOST'),
            'port' => (string) getenv('DB_PORT'),
            'database' => (string) getenv('DB_DATABASE'),
            'user' => (string) getenv('DB_USER'),
            '#password' => (string) getenv('DB_PASSWORD'),
            'schema' => (string) getenv('DB_SCHEMA'),
        ]);
    }
}