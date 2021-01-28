<?php

namespace HelloBase\Contracts;

/**
 * Interface Table
 * @package HelloBase\Contracts
 */
interface Table
{
    /**
     * @param string $row
     * @param array  $value
     * @return bool
     */
    public function put(string $row, array $value): bool;

    /**
     * @param string $row
     * @param array  $columns
     * @param null   $timestamp
     * @return array
     */
    public function row(string $row, array $columns = [], $timestamp = null): array;

    /**
     * @param array $rows
     * @param array $columns
     * @param null  $timestamp
     * @return array
     */
    public function rows(array $rows, array $columns = [], $timestamp = null): array;

    /**
     * @param string $start
     * @param string $stop
     * @param array  $columns
     * @param array  $with
     * @return mixed
     */
    public function scan(string $start = '', string $stop = '', array $columns = [], array $with = []);

    /**
     * @param string $row
     * @param string $column
     * @param int    $amount
     * @return bool
     */
    public function increment(string $row, string $column, int $amount = 1): bool;
}
