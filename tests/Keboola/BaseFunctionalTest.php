<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Keboola\DbWriter\Redshift\Test\S3Loader;
use Keboola\DbWriter\Test\BaseTest;
use Keboola\StorageApi\Client;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class BaseFunctionalTest extends BaseTest
{
    /** @var string  */
    protected const DRIVER = 'Redshift';

    /** @var string $dataDir */
    protected $dataDir = ROOT_PATH . 'tests/data/functional';

    /** @var string $tmpDataDir */
    protected $tmpDataDir = '/tmp/wr-db-redshift/data';

    public function testRun(): void
    {
        $this->prepareDataFiles($this->initConfig());
        $process = $this->runProcess();
        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());
    }

    public function testWrongColumnOrder(): void
    {
        // shuffle columns order
        $config = $this->initConfig(function ($config) {
            $col1 =  array_shift($config['storage']['input']['tables'][0]['columns']);
            array_push($config['storage']['input']['tables'][0]['columns'], $col1);
            return $config;
        });
        $this->prepareDataFiles($config);

        $process = $this->runProcess();
        $this->assertEquals(1, $process->getExitCode(), $process->getOutput());
        $this->assertStringContainsString(
            'Columns in configuration of table "simple" does not match with input mapping.' .
            ' Edit and re-save the configuration to fix the problem.',
            $process->getOutput()
        );
    }

    public function testTestConnection(): void
    {
        $config = $this->initConfig(function ($config) {
            $config['action'] = 'testConnection';
            $config['storage'] = [];
            return $config;
        });
        $this->prepareDataFiles($config);

        $process = $this->runProcess();
        $data = json_decode($process->getOutput(), true);

        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    protected function initConfig(?callable $callback = null): array
    {
        $srcConfigPath = $this->dataDir . '/config.json';
        $dstConfigPath = $this->tmpDataDir . '/config.json';
        $config = json_decode(file_get_contents($srcConfigPath), true);

        $config['parameters']['writer_class'] = self::DRIVER;
        $config['parameters']['data_dir'] = $this->tmpDataDir;
        $config['parameters']['db']['user'] = $this->getEnv(self::DRIVER . '_DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv(self::DRIVER . '_DB_PASSWORD', true);
        $config['parameters']['db']['password'] = $this->getEnv(self::DRIVER . '_DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv(self::DRIVER . '_DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv(self::DRIVER . '_DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv(self::DRIVER . '_DB_DATABASE');
        $config['parameters']['db']['schema'] = $this->getEnv(self::DRIVER . '_DB_SCHEMA');

        if ($callback !== null) {
            $config = $callback($config);
        }
        $fs = new Filesystem();
        if (file_exists($this->tmpDataDir)) {
            $fs->remove($this->tmpDataDir);
        }
        $fs->mkdir($this->tmpDataDir . '/in/tables');
        file_put_contents($dstConfigPath, json_encode($config));

        return $config;
    }

    protected function runProcess(): Process
    {
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpDataDir . ' 2>&1');
        $process->setTimeout(300);
        $process->run();

        return $process;
    }

    protected function prepareDataFiles(array $config): void
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
        if (isset($config['parameters']['tables'])) {
            $tables = $config['parameters']['tables'];
        } else {
            $tables = array($config['parameters']);
        }

        foreach ($tables as $table) {
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
}
