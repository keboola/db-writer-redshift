<?php

declare(strict_types=1);

namespace Keboola\DbWriter;

use Keboola\DbWriter\Configuration\Validator;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Redshift\Configuration\RedshiftActionConfigRowDefinition;
use Keboola\DbWriter\Redshift\Configuration\RedshiftConfigDefinition;
use Keboola\DbWriter\Redshift\Configuration\RedshiftConfigRowDefinition;
use Keboola\DbWriter\Writer\Redshift;
use Pimple\Container;

class RedshiftApplication
{
    /** @var Container $container */
    protected $container;

    public function __construct(array $config, Logger $logger)
    {
        $action = !is_null($config['action']) ? $config['action'] : 'run';
        if (isset($config['parameters']['tables'])) {
            $configDefinition = new RedshiftConfigDefinition();
        } else {
            if ($action === 'run') {
                $configDefinition = new RedshiftConfigRowDefinition();
            } else {
                $configDefinition = new RedshiftActionConfigRowDefinition();
            }
        }

        $validate = Validator::getValidator($configDefinition);
        $parameters = $validate($config['parameters']);

        $this->container = new Container();
        $this->container['action'] = $action;
        $this->container['parameters'] = $parameters;
        $this->container['inputMapping'] = $config['storage']['input']['tables'] ?? [];
        $this->container['logger'] = $logger;
        $this->container['writer'] = function ($container) {
            return new Redshift($container['parameters']['db'], $container['logger']);
        };
    }

    public function run(): array
    {
        $actionMethod = $this->container['action'] . 'Action';
        if (!method_exists($this, $actionMethod)) {
            throw new UserException(sprintf("Action '%s' does not exist.", $this->container['action']));
        }

        return $this->$actionMethod();
    }

    public function runAction(): array
    {
        $uploaded = [];
        if (isset($this->container['parameters']['tables'])) {
            $tables = array_filter($this->container['parameters']['tables'], function ($table) {
                return ($table['export']);
            });
            foreach ($tables as $tableConfig) {
                $upload = $this->runWriteTable($tableConfig);
                if (!is_null($upload)) {
                    $uploaded[] = $upload;
                }
            }
        } else {
            $upload = $this->runWriteTable($this->container['parameters']);
            if (!is_null($upload)) {
                $uploaded[] = $upload;
            }
        }
        return [
            'status' => 'success',
            'uploaded' => $uploaded,
        ];
    }

    public function runWriteTable(array $tableConfig): ?string
    {

        $manifest = $this->getManifest($tableConfig['tableId']);
        $this->checkColumns($tableConfig);

        if (empty($tableConfig['items'])) {
            return null;
        }

        try {
            if ($tableConfig['incremental']) {
                $this->loadIncremental($tableConfig, $manifest);
                return $tableConfig['tableId'];
            }

            $this->loadFull($tableConfig, $manifest);
            return $tableConfig['tableId'];
        } catch (\PDOException $e) {
            throw new UserException($e->getMessage(), 0, $e, ['trace' => $e->getTraceAsString()]);
        } catch (UserException $e) {
            throw $e;
        } catch (\Throwable $e) {
            throw new ApplicationException($e->getMessage(), 2, $e, ['trace' => $e->getTraceAsString()]);
        }
    }

    public function loadIncremental(array $tableConfig, array $manifest): void
    {
        /** @var Redshift $writer */
        $writer = $this->container['writer'];

        // write to staging table
        $stageTable = $tableConfig;
        $stageTable['dbName'] = $writer->generateTmpName($tableConfig['dbName']);

        $writer->drop($stageTable['dbName']);
        $writer->create($stageTable);
        $writer->writeFromS3($manifest['s3'], $stageTable);

        // create destination table if not exists
        if (!$writer->tableExists($tableConfig['dbName'])) {
            $writer->create($tableConfig);
        }

        // upsert from staging to destination table
        $writer->upsert($stageTable, $tableConfig['dbName']);
    }

    public function loadFull(array $tableConfig, array $manifest): void
    {
        /** @var Redshift $writer */
        $writer = $this->container['writer'];

        $writer->drop($tableConfig['dbName']);
        $writer->create($tableConfig);
        $writer->writeFromS3($manifest['s3'], $tableConfig);
    }

    private function getManifest(string $tableId): array
    {
        return json_decode(file_get_contents(
            $this->container['parameters']['data_dir'] . '/in/tables/' . $tableId . '.csv.manifest'
        ), true);
    }

    private function getInputMapping(string $tableId): array
    {
        foreach ($this->container['inputMapping'] as $inputTable) {
            if ($tableId === $inputTable['source']) {
                return $inputTable;
            }
        }

        throw new UserException(sprintf(
            'Table "%s" is missing from input mapping.' .
            ' Reloading the page and re-saving configuration may fix the problem.',
            $tableId
        ));
    }

    /**
     * Check if input mapping is aligned with table config
     *
     * @param $tableConfig
     * @throws UserException
     */
    private function checkColumns(array $tableConfig): void
    {
        $inputMapping = $this->getInputMapping($tableConfig['tableId']);
        $mappingColumns = $inputMapping['columns'];
        $tableColumns = array_map(function ($item) {
            return $item['name'];
        }, $tableConfig['items']);

        if ($mappingColumns !== $tableColumns) {
            throw new UserException(sprintf(
                'Columns in configuration of table "%s" does not match with input mapping.' .
                ' Edit and re-save the configuration to fix the problem.',
                $inputMapping['source']
            ));
        }
    }

    public function testConnectionAction(): array
    {
        try {
            $this->container['writer']->testConnection();
        } catch (\Throwable $e) {
            throw new UserException(sprintf("Connection failed: '%s'", $e->getMessage()), 0, $e);
        }

        return [
            'status' => 'success',
        ];
    }

    public function getTablesInfoAction(): array
    {
        $tables = $this->container['writer']->showTables($this['parameters']['db']['database']);

        $tablesInfo = [];
        foreach ($tables as $tableName) {
            $tablesInfo[$tableName] = $this['writer']->getTableInfo($tableName);
        }

        return [
            'status' => 'success',
            'tables' => $tablesInfo,
        ];
    }
}
