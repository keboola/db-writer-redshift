<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use ErrorException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriterAdapter\PDO\PdoConnection;
use PDO;
use PDOException;
use PDOStatement;

class RedshiftConnection extends PdoConnection
{
    public function quoteIdentifier(string $str): string
    {
        return '"' . str_replace('"', '""', $str) . '"';
    }

    /**
     * @inheritdoc
     */
    protected function doQuery(string $queryType, string $query): ?array
    {
        switch ($queryType) {
            case self::QUERY_TYPE_EXEC:
                try {
                    $this->pdo->prepare($query)->execute();
                } catch (PDOException | ErrorException $e) {
                    $exception = $this->errorsToException($query);
                    $this->logger->error($exception->getMessage());
                    throw $e;
                }
                return null;
            case self::QUERY_TYPE_FETCH_ALL:
                $stmt = $this->pdo->query($query);
                if (!$stmt instanceof PDOStatement) {
                    return [];
                }
                $result = $stmt->fetchAll(PDO::FETCH_ASSOC);
                $stmt->closeCursor();
                return $result;
            default:
                throw new UserException(sprintf('Unknown query type "%s".', $queryType));
        }
    }

    public function testConnection(): void
    {
        $this->exec('select current_date', 1);
    }

    private function errorsToException(string $query): UserException
    {
        $errors = $this->getErrors();
        $message = '';
        foreach ($errors as $error) {
            $message .= sprintf(
                "Column '%s', line %s: %s",
                trim($error['colname']),
                $error['line_number'],
                trim($error['err_reason']),
            );
        }

        return new UserException($message);
    }

    /**
     * @return array{colname: string, line_number: string, err_reason: string}[]
     */
    private function getErrors(): array
    {
        /** @var array{colname: string, line_number: string, err_reason: string}[] $errors */
        $errors = $this->fetchAll('SELECT * FROM stl_load_errors WHERE query = pg_last_query_id()-1;');
        return $errors;
    }
}
