<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 27/10/16
 * Time: 17:20
 */

namespace Keboola\DbWriter\Redshift\Tests;

use Keboola\DbWriter\Redshift\Test\S3Loader;
use Keboola\DbWriter\Test\BaseTest;
use Keboola\StorageApi\Client;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class FunctionalTest extends BaseTest
{
    const DRIVER = 'Redshift';

    protected $dataDir = ROOT_PATH . 'tests/data/functional';

    protected $tmpDataDir = '/tmp/wr-db-redshift/data';

    public function setUp()
    {
        $fs = new Filesystem();
        if (file_exists($this->tmpDataDir)) {
            $fs->remove($this->tmpDataDir);
        }
        $fs->mkdir($this->tmpDataDir . '/in/tables');
    }

    public function testRun()
    {
        $this->prepareDataFiles($this->initConfig());
        $process = $this->runProcess();
        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());
    }

    public function testBadDataType()
    {
        $config = $this->initConfig(function ($config) {
            $config['parameters']['tables'] = [[
                "tableId" => "bad_type",
                "dbName" => "bad_type",
                "export" => true,
                "incremental" => false,
                "primaryKey" => [
                    "id"
                ],
                "items" => [
                    [
                        "name" => "id",
                        "dbName" => "id",
                        "type" => "int",
                        "size" => null,
                        "nullable" => null,
                        "default" => null
                    ],
                    [
                        "name" => "name",
                        "dbName" => "name",
                        "type" => "nvarchar",
                        "size" => 255,
                        "nullable" => null,
                        "default" => null
                    ],
                    [
                        "name" => "glasses",
                        "dbName" => "glasses",
                        "type" => "nvarchar",
                        "size" => 255,
                        "nullable" => null,
                        "default" => null
                    ],
                    [
                        "name" => "created",
                        "dbName" => "created",
                        "type" => "date",
                        "size" => "",
                        "nullable" => null,
                        "default" => null
                    ]
                ]
            ]];
            $config['storage']['input']['tables'][] = [
                'source' => 'bad_type',
                'destination' => 'bad_type.csv',
                'columns' => [
                    'id',
                    'name',
                    'glasses',
                    'created'
                ]
            ];
            return $config;
        });

        $this->prepareDataFiles($config);

        $process = $this->runProcess();
        $this->assertEquals(1, $process->getExitCode(), $process->getOutput());
        $this->assertContains(
            "Column 'created', line 3: Invalid Date Format - length must be 10 or more",
            $process->getOutput()
        );
    }

    public function testWrongColumnOrder()
    {
        // shuffle columns order
        $config = $this->initConfig(function ($config) {
            $col1 =  array_shift($config['parameters']['tables'][0]['items']);
            array_push($config['parameters']['tables'][0]['items'], $col1);

            $col1 =  array_shift($config['storage']['input']['tables'][0]['columns']);
            array_push($config['storage']['input']['tables'][0]['columns'], $col1);

            return $config;
        });
        $this->prepareDataFiles($config);

        $process = $this->runProcess();
        $this->assertEquals(1, $process->getExitCode(), $process->getOutput());
        $this->assertContains(
            'Columns in configuration of table "simple" does not match columns in Storage. Edit and re-save the configuration to fix the problem.',
            $process->getOutput()
        );
    }

    public function testTestConnection()
    {
        $config = $this->initConfig(function ($config) {
            $config['action'] = 'testConnection';
            return $config;
        });
        $this->prepareDataFiles($config);

        $process = $this->runProcess();
        $data = json_decode($process->getOutput(), true);

        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    private function initConfig(callable $callback = null)
    {
        $srcConfigPath = $this->dataDir . '/config.json';
        $dstConfigPath = $this->tmpDataDir . '/config.json';
        $config = json_decode(file_get_contents($srcConfigPath), true);

        $config['parameters']['writer_class'] = self::DRIVER;
        $config['parameters']['data_dir'] = $this->tmpDataDir;
        $config['parameters']['db']['user'] = $this->getEnv(self::DRIVER, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv(self::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['password'] = $this->getEnv(self::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv(self::DRIVER, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv(self::DRIVER, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv(self::DRIVER, 'DB_DATABASE');
        $config['parameters']['db']['schema'] = $this->getEnv(self::DRIVER, 'DB_SCHEMA');


        if ($callback !== null) {
            $config = $callback($config);
        }

        @unlink($dstConfigPath);
        file_put_contents($dstConfigPath, json_encode($config));

        return $config;
    }

    private function prepareDataFiles($config)
    {
        $writer = $this->getWriter($config['parameters']);
        $s3Loader = new S3Loader(
            $this->tmpDataDir,
            new Client([
                'url' => 'https://connection.keboola.com',
                'token' => getenv('STORAGE_API_TOKEN'),
            ])
        );

        $fs = new Filesystem();
        foreach ($config['parameters']['tables'] as $table) {
            // clean destination DB
            $writer->drop($table['dbName']);

            // upload source files to S3 - mimic functionality of docker-runner
            $srcPath = $this->dataDir . '/in/tables/' . $table['tableId'] . '.csv';
            $dstPath = $this->tmpDataDir . '/in/tables/' . $table['tableId'] . '.csv';
            $fs->copy($srcPath, $dstPath);

            $manifestData = json_decode(file_get_contents($srcPath . '.manifest'), true);
            $manifestData['s3'] = $s3Loader->upload($table['tableId']);

            file_put_contents(
                $dstPath . '.manifest',
                json_encode($manifestData)
            );
        }
    }

    protected function runProcess()
    {
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpDataDir . ' 2>&1');
        $process->setTimeout(300);
        $process->run();

        return $process;
    }
}
