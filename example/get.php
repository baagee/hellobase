<?php

require __DIR__ . '/../vendor/autoload.php';

use HelloBase\Connection;

$connection = new Connection([
    'host' => '127.0.0.1',
    'port' => '9090',
    'auto_connect' => false,
    'persist' => false,
    'debug_handler' => null,
    'send_timeout' => 1000000,
    'recv_timeout' => 1000000,
    'transport' => Connection::TRANSPORT_FRAMED,
    'protocol' => Connection::PROTOCOL_COMPACT,
]);
$connection->connect();

$client = $connection->getClient();

// $filter = "QualifierFilter(>=, 'binary:1500')"; // greater than 1500
$scan = new \Hbase\TScan(array(
    'startRow' => '111222',
    'stopRow' => '123457',
    // 'filterString' => $filter, 'sortColumns' => true
));
//https://cntofu.com/book/173/docs/16.md
$table = 'emp_travel';
$scanid = $client->scannerOpenWithScan($table, $scan, []);
var_dump($scanid);
// $rowresult = $client->scannerGet($scanid);#获取一个

while ($rowresult = $client->scannerGetList($scanid, 1)) {
    var_dump(count($rowresult));
    foreach ($rowresult as $itemResult) {
        echo sprintf('rowkey:%s lon:%s lat:%s' . PHP_EOL, $itemResult->row, $itemResult->columns['info:lon']->value, $itemResult->columns['info:lat']->value);
        $scan->startRow = $itemResult->row + 1;
    }
}

$tableObj = new \HelloBase\Table($table, $connection);
//获取一个
$rowresult = $tableObj->row('692078', ['info:lon', 'info:lat']);
var_dump($rowresult);
//获取多个
$resList = $tableObj->rows(['692078', '687609'], ['info:lon', 'info:lat']);
var_dump($resList);
//扫描获取多行
$listGen = $tableObj->scan('111222', '123457', ['info:lon']);
foreach ($listGen as $rowKey => $value) {
    echo sprintf('rowKey:%s ret:%s' . PHP_EOL, $rowKey, json_encode($value));
}