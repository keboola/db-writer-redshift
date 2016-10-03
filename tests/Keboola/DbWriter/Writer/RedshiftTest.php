<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 05/11/15
 * Time: 13:33
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Redshift\Configuration\ConfigDefinition;
use Keboola\DbWriter\Test\BaseTest;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;
use Symfony\Component\Yaml\Yaml;

class RedshiftTest extends BaseTest
{
    const DRIVER = 'redshift';

    /** @var Redshift */
    private $writer;

    private $config;

    /** @var Client */
    private $storageApi;

    public function setUp()
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'wr-db-redshift');
        }

        $this->config = $this->getConfig(self::DRIVER);
        $this->config['parameters']['writer_class'] = 'Redshift';
        $this->config['parameters']['db']['schema'] = 'public';
        $this->config['parameters']['db']['password'] = $this->config['parameters']['db']['#password'];
        $this->writer = $this->getWriter($this->config['parameters']);

        $tables = $this->config['parameters']['tables'];
        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
        }

        $this->storageApi = new Client([
            'token' => getenv('STORAGE_API_TOKEN')
        ]);

        $bucketId = 'in.c-test-wr-db-redshift';
        if ($this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->dropBucket($bucketId, ['force' => true]);
        }
    }

    private function getInputCsv($tableId)
    {
        return sprintf($this->dataDir . "/in/tables/%s.csv", $tableId);
    }

    private function loadDataToS3($tableId)
    {
        $filePath = $this->getInputCsv($tableId);
        $bucketId = 'in.c-test-wr-db-redshift';
        if (!$this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->createBucket('test-wr-db-redshift', Client::STAGE_IN, "", 'snowflake');
        }

        $sourceTableId = $this->storageApi->createTable($bucketId, $tableId, new CsvFile($filePath));

        $job = $this->storageApi->exportTableAsync(
            $sourceTableId,
            [
                'gzip' => true
            ]
        );
        $fileInfo = $this->storageApi->getFile(
            $job["file"]["id"],
            (new GetFileOptions())->setFederationToken(true)
        );

        return [
            "isSliced" => $fileInfo["isSliced"],
            "region" => $fileInfo["region"],
            "bucket" => $fileInfo["s3Path"]["bucket"],
            "key" => $fileInfo["isSliced"]?$fileInfo["s3Path"]["key"] . "manifest":$fileInfo["s3Path"]["key"],
            "credentials" => [
                "access_key_id" => $fileInfo["credentials"]["AccessKeyId"],
                "secret_access_key" => $fileInfo["credentials"]["SecretAccessKey"],
                "session_token" => $fileInfo["credentials"]["SessionToken"]
            ]
        ];
    }

    public function testDrop()
    {
        $conn = $this->writer->getConnection();
        $conn->exec("CREATE TABLE dropMe (
          id INT PRIMARY KEY,
          firstname VARCHAR(30) NOT NULL,
          lastname VARCHAR(30) NOT NULL)");

        $this->writer->drop("dropMe");

        $stmt = $conn->query("
            SELECT *
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'dropMe'
        ");
        $res = $stmt->fetchAll();

        $this->assertEmpty($res);
    }

    public function testCreate()
    {
        $tables = $this->config['parameters']['tables'];

        foreach ($tables as $table) {
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

    public function testWriteAsync()
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
        $csv->writeRow(["id","name","glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($this->getInputCsv($table['tableId']), $resFilename);
    }

    public function testUpsert()
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
        $s3Manifest = $this->loadDataToS3($table['tableId'] . "_increment");
        $this->writer->create($table);
        $this->writer->writeFromS3($s3Manifest, $table);

        $this->writer->upsert($table, $targetTable['dbName']);

        $stmt = $conn->query("SELECT * FROM {$targetTable['dbName']} ORDER BY id ASC");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id", "name", "glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->getInputCsv($table['tableId'] . "_merged");

        $this->assertFileEquals($expectedFilename, $resFilename);
    }

}
