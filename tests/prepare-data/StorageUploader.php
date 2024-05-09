<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Redshift\PrepareTestsData;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;

class StorageUploader
{
    private string $dataDir;

    private Client $storageApi;

    public function __construct(string $dataDir, Client $storageApiClient)
    {
        $this->dataDir = $dataDir;
        $this->storageApi = $storageApiClient;
    }

    private function getInputCsv(string $tableId): string
    {
        return sprintf($this->dataDir . '/%s.csv', $tableId);
    }

    /**
     * @return array{
     *     isSliced: bool,
     *     region: string,
     *     bucket: string,
     *     key: string,
     *     credentials: array{
     *          access_key_id: string,
     *          secret_access_key: string,
     *          session_token: string
     *     }
     * }
     */
    public function upload(string $table): array
    {
        $filePath = $this->getInputCsv($table);
        $bucketId = 'in.c-test-wr-db-redshift';
        $tableId = $bucketId . '.' . $table;

        if (!$this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->createBucket(
                'test-wr-db-redshift',
                Client::STAGE_IN,
                '',
                'snowflake',
            );
        }
        if ($this->storageApi->tableExists($tableId)) {
            $this->storageApi->dropTable($tableId);
        }

        $sourceTableId = $this->storageApi->createTableAsync($bucketId, $table, new CsvFile($filePath));

        $job = $this->storageApi->exportTableAsync(
            $sourceTableId,
            [
                'gzip' => true,
            ],
        );
        $fileInfo = $this->storageApi->getFile(
            $job['file']['id'],
            (new GetFileOptions())->setFederationToken(true),
        );

        return [
            'isSliced' => $fileInfo['isSliced'],
            'region' => $fileInfo['region'],
            'bucket' => $fileInfo['s3Path']['bucket'],
            'key' => $fileInfo['s3Path']['key'] . 'manifest',
            'credentials' => [
                'access_key_id' => $fileInfo['credentials']['AccessKeyId'],
                'secret_access_key' => $fileInfo['credentials']['SecretAccessKey'],
                'session_token' => $fileInfo['credentials']['SessionToken'],
            ],
        ];
    }
}
