<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterAdapter\WriteAdapter;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;

class Redshift extends BaseWriter
{

    protected function createConnection(DatabaseConfig $databaseConfig): Connection
    {
        $connectionFactory = new RedshiftConnectionFactory();

        return $connectionFactory->create($databaseConfig, $this->logger);
    }

    protected function createWriteAdapter(): WriteAdapter
    {
        return new RedshiftAdapter(
            $this->connection,
            new RedshiftQueryBuilder(),
            $this->logger,
        );
    }
}
