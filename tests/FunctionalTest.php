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
        $this->runProcess();
    }

    public function testRunEmptyTable()
    {
        $config = $this->initConfig(function ($config) {
            $tables = array_map(function ($table) {
                $table['items'] = array_map(function ($item) {
                    $item['type'] = 'IGNORE';
                    return $item;
                }, $table['items']);
                return $table;
            }, $config['parameters']['tables']);
            $config['parameters']['tables'] = $tables;

            return $config;
        });
        $this->prepareDataFiles($config);

        // overwrite manifest with no columns
        $fs = new Filesystem();
        foreach ($config['parameters']['tables'] as $table) {
            // upload source files to S3 - mimic functionality of docker-runner
            $srcPath = $this->dataDir . '/in/tables/' . $table['tableId'] . '.csv';
            $dstPath = $this->tmpDataDir . '/in/tables/' . $table['tableId'] . '.csv';
            $fs->copy($srcPath, $dstPath);

            $manifestData = json_decode(file_get_contents($srcPath . '.manifest'), true);
            $manifestData['columns'] = [];

            file_put_contents(
                $dstPath . '.manifest',
                json_encode($manifestData)
            );
        }

        $this->runProcess();
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
                'token' => getenv('STORAGE_API_TOKEN')
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
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());

        return $process;
    }
}
