<?php

declare(strict_types=1);

namespace Keboola\DbWriter;

use Keboola\Component\Logger as ComponentLogger;
use Monolog\DateTimeImmutable;
use function _PHPStan_cc8d35ffb\RingCentral\Psr7\str;

class Logger extends ComponentLogger
{
    /**
     * @inheritdoc
     */
    public function addRecord(
        int $level,
        string $message,
        array $context = [],
        ?DateTimeImmutable $datetime = null,
    ): bool {
        $message = (string) preg_replace(
            '/aws_access_key_id=.*;aws_secret_access_key=.*/',
            'aws_access_key_id=***;aws_secret_access_key=***',
            $message,
        );
        return parent::addRecord($level, $message, $context, $datetime);
    }
}
