<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriterAdapter\PDO\PdoConnection;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Psr\Log\LoggerInterface;

class RedshiftConnectionFactory
{
    public function create(DatabaseConfig $databaseConfig, LoggerInterface $logger): RedshiftConnection
    {
        $connection = new RedshiftConnection(
            $logger,
            $this->generateDsn($databaseConfig),
            $databaseConfig->getUser(),
            $databaseConfig->getPassword(),
            [],
        );

        $connection->exec(sprintf('SET search_path TO "%s";', $databaseConfig->getSchema()));

        return $connection;
    }

    private function generateDsn(DatabaseConfig $databaseConfig): string
    {
        $port = $databaseConfig->hasPort() ? $databaseConfig->getPort() : '5439';
        return sprintf(
            'pgsql:host=%s;port=%s;dbname=%s;keepalives=1;keepalives_idle=60',
            $databaseConfig->getHost(),
            $port,
            $databaseConfig->getDatabase(),
        );
    }
}
