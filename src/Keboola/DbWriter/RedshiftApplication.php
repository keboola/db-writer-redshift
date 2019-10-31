<?php

declare(strict_types=1);

namespace Keboola\DbWriter;

use Keboola\DbWriter\Redshift\Configuration\RedshiftActionConfigRowDefinition;
use Keboola\DbWriter\Redshift\Configuration\RedshiftConfigDefinition;
use Keboola\DbWriter\Redshift\Configuration\RedshiftConfigRowDefinition;

class RedshiftApplication extends Application
{
    public function __construct(array $config, Logger $logger)
    {
        $action = !is_null($config['action']) ?: 'run';
        if (isset($config['parameters']['tables'])) {
            $configDefinition = new RedshiftConfigDefinition();
        } else {
            if ($action === 'run') {
                $configDefinition = new RedshiftConfigRowDefinition();
            } else {
                $configDefinition = new RedshiftActionConfigRowDefinition();
            }
        }
        parent::__construct($config, $logger, $configDefinition);
    }
}
