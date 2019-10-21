<?php

declare(strict_types=1);

use Keboola\StorageApi\Options\GetFileOptions;

define('ROOT_PATH', __DIR__);

require_once(dirname(__FILE__) . '/vendor/keboola/db-writer-common/bootstrap.php');

$arguments = getopt('i:t::p::', ['table:', 'token::', 'path::']);

if (getenv('STORAGE_API_TOKEN') !== false) {
    $arguments['token'] = getenv('STORAGE_API_TOKEN');
}

if (empty($arguments['table'])) {
    throw new \Exception('Table ID must be set');
}

if (empty($arguments['token'])) {
    throw new \Exception('Storage API Token must be set');
}

if (empty($arguments['path'])) {
    $arguments['path'] = '.';
}

$tableId = $arguments['table'];
$path = $arguments['path'];
$storage = new \Keboola\StorageApi\Client([
    'token' => $arguments['token'],
]);

$job = $storage->exportTableAsync($tableId, ['gzip' => true]);
$tableInfo = $storage->getTable($tableId);
$fileInfo = $storage->getFile(
    $job['file']['id'],
    (new GetFileOptions())->setFederationToken(true)
);

$manifest = [
    'id' => $tableInfo['id'],
    'uri' => $tableInfo['uri'],
    'name' => $tableInfo['name'],
    'primary_key' => $tableInfo['primaryKey'],
    'indexed_columns' => $tableInfo['indexedColumns'],
    'created' => $tableInfo['created'],
    'last_change_date' => $tableInfo['lastChangeDate'],
    'last_import_date' => $tableInfo['lastImportDate'],
    'rows_count' => $tableInfo['rowsCount'],
    'data_size_bytes' => $tableInfo['dataSizeBytes'],
    'is_alias' => $tableInfo['isAlias'],
    'columns' => $tableInfo['columns'],
    'attributes' => [],
];

$manifest['s3'] = [
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


file_put_contents(
    $path . '/' . $tableId . '.csv.manifest',
    json_encode($manifest)
);
