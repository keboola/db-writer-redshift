<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 29/09/16
 * Time: 17:45
 */

namespace Keboola\DbWriter\Redshift;

use \Keboola\DbWriter\Application as BaseApplication;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Writer\Redshift;
use Symfony\Component\Yaml\Yaml;

class Application extends BaseApplication
{
    public function runAction()
    {
        $uploaded = [];
        $tables = array_filter($this['parameters']['tables'], function ($table) {
            return ($table['export']);
        });

        foreach ($tables as $tableConfig) {
            $manifest = $this->getManifest($tableConfig['tableId']);
            $tableConfig['items'] = $this->reorderColumnsFromManifest($manifest['columns'], $tableConfig['items']);

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
        $writer = $this['writer'];

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
        $writer = $this['writer'];

        $writer->drop($tableConfig['dbName']);
        $writer->create($tableConfig);
        $writer->writeFromS3($manifest['s3'], $tableConfig);
    }

    private function getManifest($tableId)
    {
        return json_decode(file_get_contents(
            $this['parameters']['data_dir'] . "/in/tables/" . $tableId . ".csv.manifest"
        ), true);
    }

    private function reorderColumnsFromManifest($manifestColumns, $items)
    {
        $reordered = [];
        foreach ($manifestColumns as $manifestCol) {
            foreach ($items as $item) {
                if ($manifestCol == $item['name']) {
                    $reordered[] = $item;
                }
            }
        }
        return $reordered;
    }
}
