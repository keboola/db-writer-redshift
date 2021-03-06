<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Redshift\Test;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;

class S3Loader
{
    /** @var string $dataDir */
    private $dataDir;

    /** @var Client $storageApi */
    private $storageApi;

    public function __construct(string $dataDir, Client $storageApiClient)
    {
        $this->dataDir = $dataDir;
        $this->storageApi = $storageApiClient;
    }

    private function getInputCsv(string $tableId): string
    {
        return sprintf($this->dataDir . '/in/tables/%s.csv', $tableId);
    }

    public function upload(string $tableId): array
    {
        $filePath = $this->getInputCsv($tableId);
        $bucketId = 'in.c-test-wr-db-redshift';
        if (!$this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->createBucket('test-wr-db-redshift', Client::STAGE_IN, '', 'snowflake');
        }

        $sourceTableId = $this->storageApi->createTable($bucketId, $tableId, new CsvFile($filePath));

        $job = $this->storageApi->exportTableAsync(
            $sourceTableId,
            [
                'gzip' => true,
            ]
        );
        $fileInfo = $this->storageApi->getFile(
            $job['file']['id'],
            (new GetFileOptions())->setFederationToken(true)
        );

        return [
            'isSliced' => $fileInfo['isSliced'],
            'region' => $fileInfo['region'],
            'bucket' => $fileInfo['s3Path']['bucket'],
            'key' => $fileInfo['isSliced']?$fileInfo['s3Path']['key'] . 'manifest':$fileInfo['s3Path']['key'],
            'credentials' => [
                'access_key_id' => $fileInfo['credentials']['AccessKeyId'],
                'secret_access_key' => $fileInfo['credentials']['SecretAccessKey'],
                'session_token' => $fileInfo['credentials']['SessionToken'],
            ],
        ];
    }
}
