<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Redshift\FunctionalTests;

use Keboola\Csv\CsvFile;
use Keboola\Csv\Exception;
use Keboola\Csv\InvalidArgumentException;
use Keboola\DatadirTests\AbstractDatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecificationInterface;
use Keboola\DbWriter\Redshift\TraitTests\CloseSshTunnelsTrait;
use Keboola\DbWriter\Writer\RedshiftConnection;
use Keboola\DbWriter\Writer\RedshiftConnectionFactory;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Psr\Log\Test\TestLogger;
use RuntimeException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class DatadirTest extends AbstractDatadirTestCase
{
    use CloseSshTunnelsTrait;

    public RedshiftConnection $connection;

    protected string $testProjectDir;

    public ?string $orderResults = null;

    /**
     * @param array<mixed> $data
     */
    public function __construct(
        ?string $name = null,
        array $data = [],
        string $dataName = '',
    ) {
        parent::__construct($name, $data, $dataName);
        $databaseConfig = $this->getDatabaseConfig();
        $connectionFactory = new RedshiftConnectionFactory();
        $this->connection = $connectionFactory->create($databaseConfig, new TestLogger());
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->closeSshTunnels();
        $this->dropTables();
        $this->orderResults = null;
        $this->testProjectDir = $this->getTestFileDir() . '/' . $this->dataName();

        // Load setUp.php file - used to init database state
        $setUpPhpFile = $this->testProjectDir . '/setUp.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }

            // Invoke callback
            $initCallback($this);
        }
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        $setUpPhpFile = $this->testProjectDir . '/tearDown.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }
            $initCallback($this);
        }
    }

    /**
     * @dataProvider provideDatadirSpecifications
     */
    public function testDatadir(DatadirTestSpecificationInterface $specification): void
    {

        $tempDatadir = $this->getTempDatadir($specification);

        $finder = new Finder();
        $files = $finder
            ->files()
            ->in($this->testProjectDir . '/source/data/in/tables')
            ->name('*.manifest');

        /** @var array<string, array{'stagingStorage': string, 'manifest': array}> $manifestData */
        $manifestData = json_decode(
            (string) file_get_contents(
                __DIR__ . '/../prepare-data/manifestData.json',
            ),
            true,
        );
        foreach ($files as $file) {
            $filename = $file->getFilenameWithoutExtension();
            if (!isset($manifestData[$filename])) {
                throw new RuntimeException(sprintf('Table in storage for file "%s" not found!', $filename));
            }
            $manifestPath = sprintf(
                '%s/in/tables/%s.manifest',
                $tempDatadir->getTmpFolder(),
                $filename,
            );

            /** @var array<string, array> $manifest */
            $manifest = json_decode((string) file_get_contents($manifestPath), true);
            $manifest['s3'] = $manifestData[$filename];
            file_put_contents($manifestPath, json_encode($manifest, JSON_PRETTY_PRINT|JSON_UNESCAPED_SLASHES));
        }

        $process = $this->runScript($tempDatadir->getTmpFolder());

        $this->dumpTables($tempDatadir->getTmpFolder());

        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
    }

    private function dropTables(): void
    {
        foreach ($this->getTableNames() as $tableName) {
            $this->connection->exec(sprintf(
                'DROP TABLE IF EXISTS %s.%s',
                $this->connection->quoteIdentifier($this->getDatabaseConfig()->getSchema()),
                $this->connection->quoteIdentifier($tableName),
            ));
        }
    }

    /**
     * @throws Exception|InvalidArgumentException
     */
    private function dumpTables(string $tmpFolder): void
    {
        $dumpDir = $tmpFolder . '/out/db-dump';
        $fs = new Filesystem();
        $fs->mkdir($dumpDir);

        foreach ($this->getTableNames() as $tableName) {
            $this->dumpTableData($tableName, $dumpDir);
        }
    }

    /**
     * @return string[]
     */
    private function getTableNames(): array
    {
        $tables = $this->connection->fetchAll(
            sprintf(
                'SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = \'%s\';',
                getenv('DB_SCHEMA'),
            ),
        );

        return array_map(function ($item) {
            return $item['table_name'];
        }, $tables);
    }

    /**
     * @throws Exception|InvalidArgumentException
     */
    private function dumpTableData(string $tableName, string $tmpFolder): void
    {
        $csvDumpFile = new CsvFile(sprintf('%s/%s.csv', $tmpFolder, $tableName));
        $sql = sprintf('SELECT * FROM "%s"', $tableName);
        if ($this->orderResults) {
            $sql .= sprintf(
                ' ORDER BY %s',
                $this->connection->quoteIdentifier($this->orderResults),
            );
        }

        $rows = $this->connection->fetchAll($sql);
        if ($rows) {
            $csvDumpFile->writeRow(array_keys(current($rows)));
            foreach ($rows as $row) {
                $row = array_map(fn($v) => is_null($v) ? '' : $v, $row);
                $csvDumpFile->writeRow($row);
            }
        }
    }

    public function getDatabaseConfig(): DatabaseConfig
    {
        $config = [
            'host' =>  getenv('DB_HOST'),
            'port' => getenv('DB_PORT'),
            'user' => getenv('DB_USER'),
            '#password' => getenv('DB_PASSWORD'),
            'database' => getenv('DB_DATABASE'),
            'schema' => getenv('DB_SCHEMA'),
        ];

        return DatabaseConfig::fromArray($config);
    }
}
