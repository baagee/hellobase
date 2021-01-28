<?php

namespace HelloBase\Contracts;

/**
 * Interface Connection
 * @package HelloBase\Contracts
 */
interface Connection
{
    /**
     * @return mixed
     */
    public function connect();

    /**
     * @return mixed
     */
    public function close();

    /**
     * @param $name
     * @return Table
     */
    public function table($name): Table;

    /**
     * @return array
     */
    public function tables(): array;

    /**
     * @param       $table
     * @param array $columnFamilies
     * @return bool
     */
    public function createTable($table, array $columnFamilies): bool;
}
