<?php

namespace HelloBase;

use Exception;
use Hbase\IOError;
use Hbase\TIncrement;
use Hbase\TRowResult;
use HelloBase\Contracts\Table as TableContract;

/**
 * Class Table
 * @package HelloBase
 */
class Table implements TableContract
{
    /**
     * @var string
     */
    protected $table;
    /**
     * @var Connection
     */
    protected $connection;

    /**
     * Table constructor.
     * @param string     $table
     * @param Connection $connection
     */
    public function __construct(string $table, Connection $connection)
    {
        $this->table = $table;
        $this->connection = $connection;
    }

    /**
     * 保存一行数据
     * @param string $key
     * @param array  $values
     * @return bool
     * @throws Exception
     */
    public function put(string $key, array $values): bool
    {
        try {
            $putter = new Putter($this);
            $putter->pick($key, $values);
            return $putter->send() > 0;
        } catch (Exception $exception) {
            throw $exception;
        }
    }

    /**
     * 获取一行数据某些列
     * @param string $row
     * @param array  $columns
     * @param null   $timestamp
     * @return array
     * @throws IOError
     */
    public function row(string $row, array $columns = [], $timestamp = null): array
    {
        $client = $this->connection->getClient();

        if (is_null($timestamp)) {
            $data = $client->getRowWithColumns($this->table, $row, $columns, []);
        } else {
            $data = $client->getRowWithColumnsTs($this->table, $row, $columns, $timestamp, []);
        }

        return count($data) ? $this->formatRow($data[0]) : [];
    }

    /**
     * 获取多行数据
     * @param array $rows
     * @param array $columns
     * @param null  $timestamp
     * @return array
     * @throws IOError
     */
    public function rows(array $rows, array $columns = [], $timestamp = null): array
    {
        $client = $this->connection->getClient();

        if (!is_null($timestamp)) {
            $data = $client->getRowsWithColumnsTs($this->table, $rows, $columns, $timestamp, []);
        } else {
            $data = $client->getRowsWithColumns($this->table, $rows, $columns, []);
        }

        return $this->formatRows($data);
    }

    /**
     * 扫描获取列表 左闭右开
     * @param string $start   开始位置
     * @param string $stop    结束位置 不包括结束
     * @param array  $columns 列
     * @param array  $with    其他条件
     * @return \Generator
     * @throws IOError
     * @throws \Hbase\IllegalArgument
     */
    public function scan(string $start = '', string $stop = '', array $columns = [], array $with = [])
    {
        $client = $this->connection->getClient();

        $scannerId = $client->scannerOpenWithStop(
            $this->table,
            $start,
            $stop,
            $columns,
            $with
        );

        try {
            while ($list = $client->scannerGetList($scannerId, 50)) {
                foreach ($list as $result) {
                    yield $result->row => $this->formatRow($result);
                }
            }

            $client->scannerClose($scannerId);
        } catch (Exception $exception) {
            $client->scannerClose($scannerId);

            throw $exception;
        }
    }

    /**
     * @param string $row 自增某一列的值
     * @param string $column 列名
     * @param int    $amount 自增大小
     * @return bool
     * @throws \Hbase\IOError
     */
    public function increment(string $row, string $column, int $amount = 1): bool
    {
        $increment = new TIncrement([
            'table' => $this->table,
            'row' => $row,
            'column' => $column,
            'ammount' => $amount,
        ]);

        try {
            $this->connection->getClient()->increment($increment);
        } catch (IOError $error) {
            throw $error;
        }

        return true;
    }

    /**
     * 删除某一行某一列
     * @param string $row    行
     * @param string $column 列
     * @throws IOError
     * @return bool
     */
    public function delete(string $row, $column = '')
    {
        if (!empty($column)) {
            $this->connection->getClient()->deleteAll($this->table, $row, $column, []);
        } else {
            $this->connection->getClient()->deleteAllRow($this->table, $row, []);
        }
        return true;
    }

    /**
     * 获取当前表名
     * @return string|string
     */
    public function getTable()
    {
        return $this->table;
    }

    /**
     * @return Connection
     */
    public function getConnection(): Connection
    {
        return $this->connection;
    }

    /**
     * @param array $rows
     * @return array
     */
    protected function formatRows(array $rows)
    {
        $formatted = [];

        foreach ($rows as $row) {
            $formatted[$row->row] = $this->formatRow($row);
        }

        return $formatted;
    }

    /**
     * @param TRowResult $row
     * @return array
     */
    protected function formatRow(TRowResult $row)
    {
        $formatted = [];

        foreach ($row->columns as $column => $value) {
            $formatted[$column] = $value->value;
        }

        return $formatted;
    }
}
