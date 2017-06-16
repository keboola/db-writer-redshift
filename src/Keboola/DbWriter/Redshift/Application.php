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

        /** @var Redshift $writer */
        $writer = $this['writer'];
        foreach ($tables as $table) {
            if (!$writer->isTableValid($table)) {
                continue;
            }

            $manifest = $this->getManifest($table['tableId']);

            $targetTableName = $table['dbName'];
            if ($table['incremental']) {
                $table['dbName'] = $writer->generateTmpName($table['dbName']);
            }
            $table['items'] = $this->reorderColumnsFromManifest($manifest['columns'], $table['items']);

            if (empty($table['items'])) {
                continue;
            }

            try {
                $writer->drop($table['dbName']);
                $writer->create($table);
                $writer->writeFromS3($manifest['s3'], $table);

                if ($table['incremental']) {
                    // create target table if not exists
                    if (!$writer->tableExists($targetTableName)) {
                        $destinationTable = $table;
                        $destinationTable['dbName'] = $targetTableName;
                        $destinationTable['incremental'] = false;
                        $writer->create($destinationTable);
                    }
                    $writer->upsert($table, $targetTableName);
                }
            } catch (\PDOException $e) {
                throw new UserException($e->getMessage(), 0, $e, ["trace" => $e->getTraceAsString()]);
            } catch (UserException $e) {
                throw $e;
            } catch (\Exception $e) {
                throw new ApplicationException($e->getMessage(), 2, $e, ["trace" => $e->getTraceAsString()]);
            }

            $uploaded[] = $table['tableId'];
        }

        return [
            'status' => 'success',
            'uploaded' => $uploaded
        ];
    }

    private function getManifest($tableId)
    {
        return (new Yaml())->parse(file_get_contents(
            $this['parameters']['data_dir'] . "/in/tables/" . $tableId . ".csv.manifest"
        ));
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
