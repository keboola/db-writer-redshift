<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Redshift;

class TableDescriptions
{
    /** @var string|null $table */
    private $table;

    /** @var array<string, string> $columns keyed by destination column name */
    private $columns;

    public function __construct(?string $table, array $columns)
    {
        $this->table = $table;
        $this->columns = $columns;
    }

    public function getTable(): ?string
    {
        return $this->table;
    }

    public function getColumns(): array
    {
        return $this->columns;
    }

    public function isEmpty(): bool
    {
        return $this->table === null && $this->columns === [];
    }
}
