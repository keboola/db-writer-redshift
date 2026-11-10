<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests\Redshift;

use Keboola\DbWriter\Redshift\TableDescriptionsReader;
use PHPUnit\Framework\TestCase;

class TableDescriptionsReaderTest extends TestCase
{
    private const ITEMS = [
        ['name' => 'id', 'dbName' => 'id', 'type' => 'int'],
        ['name' => 'name', 'dbName' => 'customer_name', 'type' => 'varchar'],
        ['name' => 'secret', 'dbName' => 'secret', 'type' => 'IGNORE'],
    ];

    /**
     * @dataProvider manifestProvider
     * @param mixed $manifest
     */
    public function testRead($manifest, ?string $expectedTable, array $expectedColumns): void
    {
        $descriptions = (new TableDescriptionsReader())->read($manifest, self::ITEMS);

        $this->assertSame($expectedTable, $descriptions->getTable());
        $this->assertSame($expectedColumns, $descriptions->getColumns());
        $this->assertSame($expectedTable === null && $expectedColumns === [], $descriptions->isEmpty());
    }

    public function manifestProvider(): array
    {
        return [
            'missing or invalid manifest (decodes to null)' => [null, null, []],
            'manifest is not an object' => ['not-an-object', null, []],
            'no description, no schema' => [['id' => 'in.c-b.t', 'columns' => ['id']], null, []],
            'table description only' => [['description' => 'Orders'], 'Orders', []],
            'table description without schema keeps the table description' => [
                ['description' => 'Orders', 'columns' => ['id', 'name']],
                'Orders',
                [],
            ],
            'empty and whitespace descriptions are skipped' => [
                [
                    'description' => "  \n\t",
                    'schema' => [
                        ['name' => 'id', 'description' => ''],
                        ['name' => 'name', 'description' => '   '],
                    ],
                ],
                null,
                [],
            ],
            'column description maps to renamed dbName' => [
                ['schema' => [['name' => 'name', 'description' => 'Customer name']]],
                null,
                ['customer_name' => 'Customer name'],
            ],
            'ignored item gets no description' => [
                ['schema' => [['name' => 'secret', 'description' => 'Do not write']]],
                null,
                [],
            ],
            'schema column not in items is skipped' => [
                ['schema' => [['name' => 'unknown', 'description' => 'Nope']]],
                null,
                [],
            ],
            'malformed schema entries are skipped' => [
                ['schema' => ['id', ['description' => 'no name'], ['name' => 'id', 'description' => 5]]],
                null,
                [],
            ],
            'schema is not a list' => [['schema' => 'id'], null, []],
            'legacy metadata is ignored' => [
                [
                    'metadata' => [['key' => 'KBC.description', 'value' => 'Legacy table']],
                    'column_metadata' => ['id' => [['key' => 'KBC.description', 'value' => 'Legacy id']]],
                ],
                null,
                [],
            ],
            'table and columns' => [
                [
                    'description' => 'Orders',
                    'schema' => [
                        ['name' => 'id', 'description' => 'Primary key'],
                        ['name' => 'name', 'description' => "It's \"quoted\""],
                        ['name' => 'secret', 'description' => 'Skipped'],
                    ],
                ],
                'Orders',
                ['id' => 'Primary key', 'customer_name' => "It's \"quoted\""],
            ],
        ];
    }
}
