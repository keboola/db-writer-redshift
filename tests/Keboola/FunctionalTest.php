<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\DbWriter\Writer\Redshift;
use Keboola\StorageApi\Client;
use Symfony\Component\Filesystem\Filesystem;

class FunctionalTest extends BaseFunctionalTest
{

    public function testBadDataType(): void
    {
        $config = $this->initConfig(function ($config) {
            $config['parameters']['tables'] = [[
                'tableId' => 'bad_type',
                'dbName' => 'bad_type',
                'export' => true,
                'incremental' => false,
                'primaryKey' => [
                    'id',
                ],
                'items' => [
                    [
                        'name' => 'id',
                        'dbName' => 'id',
                        'type' => 'int',
                        'size' => null,
                        'nullable' => null,
                        'default' => null,
                    ],
                    [
                        'name' => 'name',
                        'dbName' => 'name',
                        'type' => 'nvarchar',
                        'size' => 255,
                        'nullable' => null,
                        'default' => null,
                    ],
                    [
                        'name' => 'glasses',
                        'dbName' => 'glasses',
                        'type' => 'nvarchar',
                        'size' => 255,
                        'nullable' => null,
                        'default' => null,
                    ],
                    [
                        'name' => 'created',
                        'dbName' => 'created',
                        'type' => 'date',
                        'size' => '',
                        'nullable' => null,
                        'default' => null,
                    ],
                ],
            ]];
            $config['storage']['input']['tables'][] = [
                'source' => 'bad_type',
                'destination' => 'bad_type.csv',
                'columns' => [
                    'id',
                    'name',
                    'glasses',
                    'created',
                ],
            ];
            return $config;
        });

        $this->prepareDataFiles($config);

        $process = $this->runProcess();
        $this->assertEquals(1, $process->getExitCode(), $process->getOutput());
        $this->assertStringContainsString(
            "Column 'created', line 3: Invalid Date Format - length must be 10 or more",
            $process->getOutput()
        );
    }

    public function testFullLoadWritesDescriptionsAsComments(): void
    {
        $config = $this->initDescribedConfig(['incremental' => false]);
        $this->prepareDescribedData($config);

        $process = $this->runProcess();

        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());
        $this->assertSame('People and their glasses', $this->getTableComment($config, 'described'));
        $this->assertSame(
            ['full_name' => 'Person\'s "full" name', 'id' => 'Person id'],
            $this->getColumnComments($config, 'described')
        );
    }

    public function testIncrementalKeepsCommentWithoutDescription(): void
    {
        $config = $this->initDescribedConfig(['incremental' => true, 'primaryKey' => ['id']]);
        $this->prepareDescribedData($config);

        /** @var Redshift $writer */
        $writer = $this->getWriter($config['parameters']);
        $writer->create($config['parameters']['tables'][0]);
        $writer->getConnection()->exec("COMMENT ON COLUMN described.glasses IS 'Manual comment'");
        $writer->getConnection()->exec("COMMENT ON COLUMN described.full_name IS 'Old comment'");

        $process = $this->runProcess();

        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());
        $this->assertSame('People and their glasses', $this->getTableComment($config, 'described'));
        $this->assertSame(
            ['full_name' => 'Person\'s "full" name', 'glasses' => 'Manual comment', 'id' => 'Person id'],
            $this->getColumnComments($config, 'described')
        );
    }

    public function testPropagateDescriptionsOffWritesNoComments(): void
    {
        $config = $this->initDescribedConfig(['incremental' => false, 'propagateDescriptions' => false]);
        $this->prepareDescribedData($config);

        $process = $this->runProcess();

        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());
        $this->assertNull($this->getTableComment($config, 'described'));
        $this->assertSame([], $this->getColumnComments($config, 'described'));
    }

    private function initDescribedConfig(array $tableOverride): array
    {
        return $this->initConfig(function ($config) use ($tableOverride) {
            $item = function (string $name, string $dbName, string $type, ?int $size): array {
                return [
                    'name' => $name,
                    'dbName' => $dbName,
                    'type' => $type,
                    'size' => $size,
                    'nullable' => null,
                    'default' => null,
                ];
            };
            $config['parameters']['tables'] = [array_merge([
                'tableId' => 'described',
                'dbName' => 'described',
                'export' => true,
                'items' => [
                    $item('id', 'id', 'int', null),
                    $item('name', 'full_name', 'nvarchar', 255),
                    $item('glasses', 'glasses', 'nvarchar', 255),
                ],
            ], $tableOverride)];
            $config['storage']['input']['tables'] = [[
                'source' => 'described',
                'destination' => 'described.csv',
                'columns' => ['id', 'name', 'glasses'],
            ]];
            return $config;
        });
    }

    private function prepareDescribedData(array $config): void
    {
        $storageApi = new Client([
            'url' => 'https://connection.keboola.com',
            'token' => getenv('STORAGE_API_TOKEN'),
        ]);
        // S3Loader creates the Storage table on every upload
        if ($storageApi->bucketExists('in.c-test-wr-db-redshift')) {
            $storageApi->dropBucket('in.c-test-wr-db-redshift', ['force' => true]);
        }
        $this->prepareDataFiles($config);
    }

    private function getTableComment(array $config, string $table): ?string
    {
        $stmt = $this->getWriter($config['parameters'])->getConnection()->prepare(
            'SELECT d.description FROM pg_catalog.pg_description d
            JOIN pg_catalog.pg_class c ON c.oid = d.objoid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ? AND c.relname = ? AND d.objsubid = 0'
        );
        $stmt->execute([$config['parameters']['db']['schema'], $table]);
        $comment = $stmt->fetchColumn();
        return $comment === false ? null : $comment;
    }

    private function getColumnComments(array $config, string $table): array
    {
        $stmt = $this->getWriter($config['parameters'])->getConnection()->prepare(
            'SELECT a.attname, d.description FROM pg_catalog.pg_description d
            JOIN pg_catalog.pg_class c ON c.oid = d.objoid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.objsubid
            WHERE n.nspname = ? AND c.relname = ? AND d.objsubid > 0
            ORDER BY a.attname'
        );
        $stmt->execute([$config['parameters']['db']['schema'], $table]);
        return $stmt->fetchAll(\PDO::FETCH_KEY_PAIR);
    }
}
