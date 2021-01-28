<?php

require __DIR__ . '/../vendor/autoload.php';

use HelloBase\Connection;
use Hbase\IOError;

$logFunc = function ($msg) {
    echo sprintf('[%s] %s' . PHP_EOL, date('Y-m-d H:i:s'), $msg);
};

$connection = new Connection([
    'host' => '127.0.0.1',
    'port' => '9090',
    'auto_connect' => false,
    'persist' => false,
    'debug_handler' => $logFunc,
    'send_timeout' => 1000000,
    'recv_timeout' => 1000000,
    'transport' => Connection::TRANSPORT_FRAMED,
    'protocol' => Connection::PROTOCOL_COMPACT,
    'connect_retry' => 2,#重试次数
]);
$connection->connect();

try {
    $tableList = $connection->tables();
} catch (IOError $e) {
    exit($e->getMessage());
}

$client = $connection->getClient();

$tableName = 'test_table2';
if (in_array($tableName, $tableList)) {
    $deleteRes = $connection->deleteTable($tableName);
}
$res = $connection->createTable($tableName, ['info:']);
var_dump($res);
// $table = 'emp_travel';
$table = $connection->table($tableName);
for ($i = 0; $i <= 10; $i++) {
    $key = $table->put(mt_rand(111222, 999999), ['info:lon' => mt_rand(1200000, 1400000) / 10000, 'info:lat' => mt_rand(240000, 400000) / 10000]);
}

foreach ($table->scan() as $row => $columns) {
    var_dump($row, $columns);
}

$raw = $table->row($key);

var_dump($raw);


foreach ($table->scan('111222', '999999', ['info:lon']) as $row => $columns) {
    var_dump($row, $columns);
}

$deleteRes = $connection->deleteTable($tableName);
echo '删除表成功';
$connection->close();
die;
