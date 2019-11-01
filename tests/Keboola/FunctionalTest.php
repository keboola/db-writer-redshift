<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Symfony\Component\Filesystem\Filesystem;

class FunctionalTest extends BaseFunctionalTest
{

    public function setUp(): void
    {
        $fs = new Filesystem();
        if (file_exists($this->tmpDataDir)) {
            $fs->remove($this->tmpDataDir);
        }
        $fs->mkdir($this->tmpDataDir . '/in/tables');
    }

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
}
