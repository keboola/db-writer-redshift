<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Aws\S3\S3Client;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriterAdapter\BaseWriteAdapter;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;

/**
 * @property RedshiftQueryBuilder $queryBuilder
 */
class RedshiftAdapter extends BaseWriteAdapter
{
    public function writeData(string $tableName, ExportConfig $exportConfig): void
    {
        $manifestPath = $exportConfig->getTableFilePath() . '.manifest';

        /** @var array{
         *    s3?: array{
         *        bucket: string,
         *        key: string,
         *        credentials: array{
         *          access_key_id: string,
         *          secret_access_key: string,
         *          session_token: string
         *        },
         *        region: string,
         *        isSliced?: bool
         *    }
         * } $manifest
         */
        $manifest = json_decode((string) file_get_contents($manifestPath), true);
        if (!isset($manifest['s3'])) {
            throw new UserException('S3 section not found in manifest');
        }

        $s3info = $manifest['s3'];

        $s3key = $s3info['bucket'] . '/' . $s3info['key'];

        if (isset($s3info['isSliced']) && $s3info['isSliced'] === true) {
            // empty manifest handling
            $manifest = $this->downloadManifest($s3info, "s3://{$s3key}");

            if (!count($manifest['entries'])) {
                return;
            }
        }

        $this->logger->info(sprintf(
            'Writing data to table "%s"',
            $tableName,
        ));
        $this->connection->exec(
            $this->queryBuilder->writeDataFromS3QueryStatement($tableName, $s3info),
        );
    }

    public function tableExists(string $tableName): bool
    {
        return parent::tableExists(strtolower($tableName));
    }

    /**
     * @param ItemConfig[] $items
     */
    public function validateTable(string $tableName, array $items): void
    {
    }

    /**
     * @param array{
     *     credentials: array{
     *          access_key_id: string,
     *          secret_access_key: string,
     *          session_token: string
     *     },
     *     region: string
     * } $s3Info
     * @return array{entries: array{url: string}[]}
     */
    private function downloadManifest(array $s3Info, string $path): array
    {
        $s3Client = new S3Client([
            'credentials' => [
                'key' => $s3Info['credentials']['access_key_id'],
                'secret' => $s3Info['credentials']['secret_access_key'],
                'token' =>  $s3Info['credentials']['session_token'],
            ],
            'region' => $s3Info['region'],
            'version' => '2006-03-01',
        ]);
        $path = parse_url($path);
        assert(isset($path['host']));
        assert(isset($path['path']));
        /** @var array{Body: string} $response */
        $response = $s3Client->getObject([
            'Bucket' => $path['host'],
            'Key' => ltrim($path['path'], '/'),
        ]);
        /** @var array{entries: array{url: string}[]} $responseBody */
        $responseBody = json_decode((string) $response['Body'], true);
        return $responseBody;
    }
}
