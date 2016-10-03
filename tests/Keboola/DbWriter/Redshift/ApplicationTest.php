<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 30/09/16
 * Time: 23:09
 */
namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Redshift\Application;
use Keboola\DbWriter\Redshift\Configuration\ConfigDefinition;
use Keboola\DbWriter\Test\BaseTest;

class ApplicationTest extends BaseTest
{
    private $config;

    public function setUp()
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'wr-db-redshift');
        }

        $this->config = $this->getConfig('redshift');
        $this->config['parameters']['writer_class'] = 'Redshift';
        $this->config['parameters']['db']['schema'] = 'public';
    }

    public function testRun()
    {
        $app = new Application($this->config, new Logger('test'), new ConfigDefinition());
//        $app->run();
    }

}
