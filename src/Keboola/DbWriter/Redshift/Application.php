<?php

namespace Keboola\DbWriter\Redshift;

use Keboola\DbWriter\Configuration\Validator;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Redshift\Configuration\ConfigDefinition;
use Keboola\DbWriter\Writer\Redshift;
use Pimple\Container;

class Application
{
    protected $container;

    public function __construct($config, Logger $logger, $configDefinition = null)
    {
        if ($configDefinition == null) {
            $configDefinition = new ConfigDefinition();
        }

        $validate = Validator::getValidator($configDefinition);
        $parameters = $validate($config['parameters']);

        $this->container = new Container();
        $this->container['action'] = isset($config['action']) ? $config['action'] : 'run';
        $this->container['parameters'] = $parameters;
        $this->container['inputMapping'] = $config['storage']['input']['tables'] ?? [];
        $this->container['logger'] = $logger;
        $this->container['writer'] = function ($container) {
            return new Redshift($container['parameters']['db'], $container['logger']);
        };
    }

    public function run()
    {
        $actionMethod = $this->container['action'] . 'Action';
        if (!method_exists($this, $actionMethod)) {
            throw new UserException(sprintf("Action '%s' does not exist.", $this->container['action']));
        }

        return $this->$actionMethod();
    }

    public function runAction()
    {
        $uploaded = [];
        $tables = array_filter($this->container['parameters']['tables'], function ($table) {
            return ($table['export']);
        });

        foreach ($tables as $tableConfig) {
            $manifest = $this->getManifest($tableConfig['tableId']);
            $this->checkColumns($tableConfig);

            if (empty($tableConfig['items'])) {
                continue;
            }

            try {
                if ($tableConfig['incremental']) {
                    $this->loadIncremental($tableConfig, $manifest);
                    $uploaded[] = $tableConfig['tableId'];
                    continue;
                }

                $this->loadFull($tableConfig, $manifest);
                $uploaded[] = $tableConfig['tableId'];
            } catch (\PDOException $e) {
                throw new UserException($e->getMessage(), 0, $e, ["trace" => $e->getTraceAsString()]);
            } catch (UserException $e) {
                throw $e;
            } catch (\Exception $e) {
                throw new ApplicationException($e->getMessage(), 2, $e, ["trace" => $e->getTraceAsString()]);
            }
        }

        return [
            'status' => 'success',
            'uploaded' => $uploaded
        ];
    }

    public function loadIncremental($tableConfig, $manifest)
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

    public function loadFull($tableConfig, $manifest)
    {
        /** @var Redshift $writer */
        $writer = $this->container['writer'];

        $writer->drop($tableConfig['dbName']);
        $writer->create($tableConfig);
        $writer->writeFromS3($manifest['s3'], $tableConfig);
    }

    private function getManifest($tableId)
    {
        return json_decode(file_get_contents(
            $this->container['parameters']['data_dir'] . "/in/tables/" . $tableId . ".csv.manifest"
        ), true);
    }

    private function getInputMapping($tableId)
    {
        foreach ($this->container['inputMapping'] as $inputTable) {
            if ($tableId == $inputTable['source']) {
                return $inputTable;
            }
        }

        throw new UserException(sprintf(
            'Table "%s" is missing from input mapping. Reloading the page and re-saving configuration may fix the problem.',
            $tableId
        ));
    }

    /**
     * Check if input mapping is aligned with table config
     *
     * @param $tableConfig
     * @throws UserException
     */
    private function checkColumns($tableConfig)
    {
        $inputMapping = $this->getInputMapping($tableConfig['tableId']);
        $mappingColumns = $inputMapping['columns'];
        $tableColumns = array_map(function ($item) {
            return $item['name'];
        }, $tableConfig['items']);

        if ($mappingColumns !== $tableColumns) {
            throw new UserException(sprintf(
                'Columns in configuration of table "%s" does not match with input mapping. Edit and re-save the configuration to fix the problem.',
                $inputMapping['source']
            ));
        }
    }

    public function testConnectionAction()
    {
        try {
            $this->container['writer']->testConnection();
        } catch (\Exception $e) {
            throw new UserException(sprintf("Connection failed: '%s'", $e->getMessage()), 0, $e);
        }

        return [
            'status' => 'success',
        ];
    }

    public function getTablesInfoAction()
    {
        $tables = $this->container['writer']->showTables($this['parameters']['db']['database']);

        $tablesInfo = [];
        foreach ($tables as $tableName) {
            $tablesInfo[$tableName] = $this['writer']->getTableInfo($tableName);
        }

        return [
            'status' => 'success',
            'tables' => $tablesInfo
        ];
    }
}
