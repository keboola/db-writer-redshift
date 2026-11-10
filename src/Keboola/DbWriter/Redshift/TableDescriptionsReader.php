<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Redshift;

class TableDescriptionsReader
{
    /**
     * Reads native Storage descriptions (`description`, `schema[].description`) from a decoded input
     * table manifest. Column descriptions are keyed by the item's `dbName`.
     *
     * @param mixed $manifest
     */
    public function read($manifest, array $items): TableDescriptions
    {
        if (!is_array($manifest)) {
            return new TableDescriptions(null, []);
        }

        $tableDescription = $this->nonEmptyString($manifest['description'] ?? null);

        $columnDescriptions = [];
        $schema = $manifest['schema'] ?? null;
        if (is_array($schema)) {
            foreach ($schema as $column) {
                if (!is_array($column)) {
                    continue;
                }
                $name = $column['name'] ?? null;
                $description = $this->nonEmptyString($column['description'] ?? null);
                if (!is_string($name) || $description === null) {
                    continue;
                }
                foreach ($items as $item) {
                    // same case-insensitive check as Redshift::create(), so only existing columns get a comment
                    if (($item['name'] ?? null) === $name && strtolower((string) $item['type']) !== 'ignore') {
                        $columnDescriptions[(string) $item['dbName']] = $description;
                    }
                }
            }
        }

        return new TableDescriptions($tableDescription, $columnDescriptions);
    }

    /**
     * @param mixed $value
     */
    private function nonEmptyString($value): ?string
    {
        return is_string($value) && trim($value) !== '' ? $value : null;
    }
}
