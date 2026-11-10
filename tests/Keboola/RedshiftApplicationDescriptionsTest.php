<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Redshift\TableDescriptions;
use Keboola\DbWriter\RedshiftApplication;
use Keboola\DbWriter\Writer\Redshift;
use Monolog\Handler\TestHandler;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;

class RedshiftApplicationDescriptionsTest extends TestCase
{
    private const MANIFEST = [
        'description' => 'Orders',
        'schema' => [
            ['name' => 'id', 'description' => 'Primary key'],
            ['name' => 'name', 'description' => 'Customer name'],
        ],
        's3' => ['bucket' => 'b', 'key' => 'k'],
    ];

    /** @var string $dataDir */
    private $dataDir;

    /** @var TestHandler $logHandler */
    private $logHandler;

    /** @var string[] $calls */
    private $calls = [];

    protected function setUp(): void
    {
        $this->dataDir = sys_get_temp_dir() . '/wr-redshift-descriptions-' . uniqid();
        (new Filesystem())->mkdir($this->dataDir . '/in/tables');
        $this->logHandler = new TestHandler();
    }

    protected function tearDown(): void
    {
        (new Filesystem())->remove($this->dataDir);
    }

    public function testFullLoadAppliesDescriptionsToTargetAfterLoad(): void
    {
        $writer = $this->createWriter();
        $writer->expects($this->once())
            ->method('applyDescriptions')
            ->with('orders', $this->equalTo(new TableDescriptions('Orders', [
                'id' => 'Primary key',
                'customer_name' => 'Customer name',
            ])));

        $result = $this->runFirstTable($this->createApplication($writer, []));

        $this->assertSame('in.c-b.orders', $result);
        $this->assertSame(['drop', 'create', 'writeFromS3', 'applyDescriptions'], $this->calls);
    }

    public function testIncrementalAppliesDescriptionsToTargetNotStaging(): void
    {
        $writer = $this->createWriter();
        $writer->method('generateTmpName')->willReturn('orders_temp_1');
        $writer->expects($this->once())->method('applyDescriptions')->with('orders', $this->anything());

        $this->runFirstTable($this->createApplication($writer, ['incremental' => true]));

        $this->assertSame('applyDescriptions', end($this->calls));
        $this->assertContains('upsert', $this->calls);
    }

    public function testToggleOffWritesNothing(): void
    {
        $writer = $this->createWriter();
        $writer->expects($this->never())->method('applyDescriptions');

        $this->runFirstTable($this->createApplication($writer, ['propagateDescriptions' => false]));
    }

    public function testNoDescriptionsWritesNothing(): void
    {
        $writer = $this->createWriter();
        $writer->expects($this->never())->method('applyDescriptions');

        $this->runFirstTable($this->createApplication($writer, [], ['s3' => self::MANIFEST['s3']]));
    }

    public function testFailureIsLoggedAsWarningAndJobSucceeds(): void
    {
        $writer = $this->createWriter();
        $writer->method('applyDescriptions')->willThrowException(new \PDOException('must be owner of relation'));

        $result = $this->runFirstTable($this->createApplication($writer, []));

        $this->assertSame('in.c-b.orders', $result);
        $this->assertTrue($this->logHandler->hasWarningThatContains(
            'Descriptions were not written to table "orders": must be owner of relation'
        ));
    }

    public function testDefaultsToOnWhenOptionMissing(): void
    {
        $app = $this->createApplication($this->createWriter(), []);

        $this->assertTrue($app->getParameters()['tables'][0]['propagateDescriptions']);
    }

    public function testRowConfigAcceptsOption(): void
    {
        $writer = $this->createWriter();
        $writer->expects($this->never())->method('applyDescriptions');

        $row = $this->tableConfig(['propagateDescriptions' => false]);
        $app = $this->createApplicationFromParameters($writer, $row);
        $app->runWriteTable($app->getParameters());
    }

    private function runFirstTable(object $app): ?string
    {
        return $app->runWriteTable($app->getParameters()['tables'][0]);
    }

    /**
     * @return Redshift|MockObject
     */
    private function createWriter(): MockObject
    {
        $writer = $this->createMock(Redshift::class);
        foreach (['drop', 'create', 'writeFromS3', 'upsert', 'applyDescriptions'] as $method) {
            $writer->method($method)->willReturnCallback(function () use ($method): void {
                $this->calls[] = $method;
            });
        }
        return $writer;
    }

    private function tableConfig(array $override): array
    {
        return array_merge([
            'tableId' => 'in.c-b.orders',
            'dbName' => 'orders',
            'items' => [
                ['name' => 'id', 'dbName' => 'id', 'type' => 'int'],
                ['name' => 'name', 'dbName' => 'customer_name', 'type' => 'varchar'],
            ],
        ], $override);
    }

    private function createApplication(MockObject $writer, array $tableOverride, ?array $manifest = null): object
    {
        return $this->createApplicationFromParameters(
            $writer,
            ['tables' => [$this->tableConfig($tableOverride)]],
            $manifest
        );
    }

    private function createApplicationFromParameters(
        MockObject $writer,
        array $parameters,
        ?array $manifest = null
    ): object {
        file_put_contents(
            $this->dataDir . '/in/tables/in.c-b.orders.csv.manifest',
            json_encode($manifest ?? self::MANIFEST)
        );

        $config = [
            'parameters' => array_merge([
                'data_dir' => $this->dataDir,
                'writer_class' => 'Redshift',
                'db' => ['database' => 'db', 'user' => 'u'],
            ], $parameters),
            'storage' => ['input' => ['tables' => [
                ['source' => 'in.c-b.orders', 'columns' => ['id', 'name']],
            ]]],
        ];

        $logger = new Logger('test');
        $logger->setHandlers([$this->logHandler]);

        return new class($config, $logger, $writer) extends RedshiftApplication {
            public function __construct(array $config, Logger $logger, Redshift $writer)
            {
                parent::__construct($config, $logger);
                $this->container['writer'] = $writer;
            }

            public function getParameters(): array
            {
                return $this->container['parameters'];
            }
        };
    }
}
