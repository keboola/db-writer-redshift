<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests;

use Symfony\Component\Filesystem\Filesystem;

class FunctionalRowTest extends BaseFunctionalTest
{
    /** @var string $dataDir */
    protected $dataDir = ROOT_PATH . 'tests/data/functionalRow';

    public function setUp(): void
    {
        $fs = new Filesystem();
        if (file_exists($this->tmpDataDir)) {
            $fs->remove($this->tmpDataDir);
        }
        $fs->mkdir($this->tmpDataDir . '/in/tables');
    }
}
