<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests\Writer;

use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Redshift\TableDescriptions;
use Keboola\DbWriter\Writer\Redshift;
use Monolog\Handler\TestHandler;
use PHPUnit\Framework\TestCase;

class RedshiftApplyDescriptionsTest extends TestCase
{
    /** @var string[] $executed */
    private $executed = [];

    public function testCommentsUseQuotedIdentifiersAndLiterals(): void
    {
        $writer = $this->createWriter();

        $writer->applyDescriptions('my "table"', new TableDescriptions("It's a table", [
            'id' => 'Primary key',
            'we"ird' => "O'Brien",
        ]));

        $this->assertSame([
            'COMMENT ON TABLE "my ""table""" IS \'It\'\'s a table\'',
            'COMMENT ON COLUMN "my ""table"""."id" IS \'Primary key\'',
            'COMMENT ON COLUMN "my ""table"""."we""ird" IS \'O\'\'Brien\'',
        ], $this->executed);
    }

    public function testColumnsOnlyDoesNotCommentTable(): void
    {
        $writer = $this->createWriter();

        $writer->applyDescriptions('t', new TableDescriptions(null, ['c' => 'Column']));

        $this->assertSame(['COMMENT ON COLUMN "t"."c" IS \'Column\''], $this->executed);
    }

    public function testLongDescriptionIsNotTruncated(): void
    {
        $writer = $this->createWriter();
        $long = str_repeat('a', 1000);

        $writer->applyDescriptions('t', new TableDescriptions($long, []));

        $this->assertSame([sprintf('COMMENT ON TABLE "t" IS \'%s\'', $long)], $this->executed);
    }

    public function testExecFailurePropagates(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('quote')->willReturn("'x'");
        $pdo->expects($this->once())->method('exec')->willThrowException(new \PDOException('must be owner'));

        $this->expectException(\PDOException::class);
        $this->createWriter($pdo)->applyDescriptions('t', new TableDescriptions('x', ['a' => 'b']));
    }

    private function createWriter(?\PDO $pdo = null): Redshift
    {
        if ($pdo === null) {
            $pdo = $this->createMock(\PDO::class);
            $pdo->method('quote')->willReturnCallback(function (string $value): string {
                return "'" . str_replace("'", "''", $value) . "'";
            });
            $pdo->method('exec')->willReturnCallback(function (string $query): int {
                $this->executed[] = $query;
                return 0;
            });
        }

        $logger = new Logger('test');
        $logger->setHandlers([new TestHandler()]);

        return new class($pdo, $logger) extends Redshift {
            /** @var \PDO $pdo */
            private $pdo;

            public function __construct(\PDO $pdo, Logger $logger)
            {
                $this->pdo = $pdo;
                parent::__construct([], $logger);
            }

            public function createConnection(array $dbParams): \PDO
            {
                return $this->pdo;
            }
        };
    }
}
