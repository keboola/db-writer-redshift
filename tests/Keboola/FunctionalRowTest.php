<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Symfony\Component\Filesystem\Filesystem;

class FunctionalRowTest extends BaseFunctionalTest
{
    /** @var string $dataDir */
    protected $dataDir = ROOT_PATH . 'tests/data/functionalRow';

    /** @var string $tmpDataDir */
    protected $tmpDataDir = '/tmp/wr-db-redshift/dataRow';
}
