<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Redshift\Test\S3Loader;
use Keboola\DbWriter\Test\BaseTest;
use Keboola\DbWriter\Writer\Redshift;
use Keboola\StorageApi\Client;

class RedshiftTest extends BaseTest
{
    /** @var string $dataDir */
    protected $dataDir = __DIR__ . '/../../data';

    private const DRIVER = 'redshift';

    /** @var Redshift $writer */
    private $writer;

    /** @var array $config  */
    private $config;

    /** @var Client $storageApi */
    private $storageApi;

    /** @var S3Loader $s3Loader */
    private $s3Loader;

    public function setUp(): void
    {
        $this->config = $this->getConfig($this->dataDir . '/' . self::DRIVER);
        $this->config['parameters']['writer_class'] = 'Redshift';
        $this->config['parameters']['db']['schema'] = 'public';
        $this->config['parameters']['db']['password'] = $this->config['parameters']['db']['#password'];
        $this->writer = $this->getWriter($this->config['parameters']);

        $tables = $this->config['parameters']['tables'];
        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
        }

        $this->storageApi = new Client([
            'url' => getenv('STORAGE_API_URL'),
            'token' => getenv('STORAGE_API_TOKEN'),
        ]);

        $bucketId = 'in.c-test-wr-db-redshift';
        if ($this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->dropBucket($bucketId, ['force' => true]);
        }

        $this->s3Loader = new S3Loader($this->dataDir, $this->storageApi);
    }

    protected function getConfig(?string $dataDir = null): array
    {
        $dataDir = $dataDir ?: $this->dataDir;
        $config = json_decode(file_get_contents($dataDir . '/config.json'), true);
        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['db']['user'] = $this->getEnv(self::DRIVER . '_DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv(self::DRIVER . '_DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv(self::DRIVER . '_DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv(self::DRIVER . '_DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv(self::DRIVER . '_DB_DATABASE');

        return $config;
    }

    private function getInputCsv(string $tableId): string
    {
        return sprintf($this->dataDir . '/in/tables/%s.csv', $tableId);
    }

    private function loadDataToS3(string $tableId): array
    {
        return $this->s3Loader->upload($tableId);
    }

    public function testDrop(): void
    {
        $conn = $this->writer->getConnection();
        $conn->exec('CREATE TABLE dropMe (
          id INT PRIMARY KEY,
          firstname VARCHAR(30) NOT NULL,
          lastname VARCHAR(30) NOT NULL)');

        $this->writer->drop('dropMe');

        $stmt = $conn->query("
            SELECT *
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'dropMe'
        ");
        $res = $stmt->fetchAll();

        $this->assertEmpty($res);
    }

    public function testCreate(): void
    {
        $tables = $this->config['parameters']['tables'];

        foreach ($tables as $table) {
            $table['incremental'] = false;
            $this->writer->drop($table['dbName']);
            $this->writer->create($table);
        }

        /** @var \PDO $conn */
        $conn = $this->writer->getConnection();
        $stmt = $conn->query("
            SELECT *
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = '{$tables[0]['dbName']}'
        ");
        $res = $stmt->fetchAll();

        $this->assertEquals('simple', $res[0]['table_name']);
    }

    public function testWriteAsync(): void
    {
        $tables = $this->config['parameters']['tables'];

        // simple table
        $table = $tables[0];
        $s3manifest = $this->loadDataToS3($table['tableId']);

        $this->writer->drop($table['dbName']);
        $this->writer->create($table);
        $this->writer->writeFromS3($s3manifest, $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM {$table['dbName']} ORDER BY id ASC");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(['id','name','glasses']);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($this->getInputCsv($table['tableId']), $resFilename);
    }

    public function testWriteEmpty(): void
    {
        $tables = $this->config['parameters']['tables'];
        $tables = array_filter($tables, function ($table) {
            return $table['tableId'] === 'empty';
        });

        // empty table
        $table = reset($tables);
        $s3manifest = $this->loadDataToS3($table['tableId']);

        $this->writer->drop($table['dbName']);
        $this->writer->create($table);
        $this->writer->writeFromS3($s3manifest, $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM {$table['dbName']} ORDER BY id ASC");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(['id','name']);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($this->getInputCsv($table['tableId']), $resFilename);
    }

    public function testUpsert(): void
    {
        $conn = $this->writer->getConnection();
        $tables = $this->config['parameters']['tables'];
        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
        }
        $table = $tables[0];

        $s3Manifest = $this->loadDataToS3($table['tableId']);

        $targetTable = $table;
        $table['dbName'] .= $table['incremental']?'_temp_' . uniqid():'';

        // first write
        $this->writer->create($targetTable);
        $this->writer->writeFromS3($s3Manifest, $targetTable);

        // second write
        $s3Manifest = $this->loadDataToS3($table['tableId'] . '_increment');
        $this->writer->create($table);
        $this->writer->writeFromS3($s3Manifest, $table);

        $this->writer->upsert($table, $targetTable['dbName']);

        $stmt = $conn->query("SELECT * FROM {$targetTable['dbName']} ORDER BY id ASC");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(['id', 'name', 'glasses']);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->getInputCsv($table['tableId'] . '_merged');

        $this->assertFileEquals($expectedFilename, $resFilename);
    }
}
