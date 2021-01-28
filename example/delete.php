<?php

require __DIR__ . '/../vendor/autoload.php';

use HelloBase\Connection;

$connection = new Connection([
    'host' => '127.0.0.1',
    'port' => '9090',
    'auto_connect' => true,
    'persist' => false,
    'debug_handler' => null,
    'send_timeout' => 1000000,
    'recv_timeout' => 1000000,
    'transport' => Connection::TRANSPORT_FRAMED,
    'protocol' => Connection::PROTOCOL_COMPACT,
    // 'transport' => Connection::TRANSPORT_FRAMED,
    // 'protocol' => Connection::PROTOCOL_BINARY,
]);
$connection->connect();

$client = $connection->getClient();

$table = 'emp_travel';
$tableObj = new \HelloBase\Table($table, $connection);
$tableObj->delete('199501');

$tableObj->delete('111222', 'info:lon');

try {
    $res = $connection->createTable('dlh_test', ['info:']);
    var_dump($res);
} catch (Exception $e) {
    var_dump($e->getMessage());
}

var_dump($connection->deleteTable('dlh_test'));
var_dump('over');
