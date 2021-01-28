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

//单个操作
$data = array(new \Hbase\Mutation(array('column' => 'info:lon',
                                        'value' => mt_rand(1201234, 1512340) / 10000)));
$table = 'emp_travel';

$loginId = mt_rand(111223, 999999);
$client->mutaterow($table, $loginId, $data, []);

echo '开始批量操作' . PHP_EOL;

$data = [];
for ($i = 0; $i <= 2; $i++) {
    $data[] = new \Hbase\BatchMutation(['row' => mt_rand(111223, 999999), 'mutations' => [
        new \Hbase\Mutation([
            'column' => 'info:lon', 'value' => mt_rand(1201234, 1512340) / 10000, 'isDelete' => false
        ]),
        new \Hbase\Mutation([
            'column' => 'info:lat', 'value' => mt_rand(321234, 402340) / 10000, 'isDelete' => false
        ]),
    ]]);
    $data[] = new \Hbase\BatchMutation(['row' => mt_rand(111223, 999999), 'mutations' => [
        new \Hbase\Mutation([
            'column' => 'info:lon', 'value' => mt_rand(1201234, 1512340) / 10000, 'isDelete' => false
        ]),
        new \Hbase\Mutation([
            'column' => 'info:lat', 'value' => mt_rand(321234, 402340) / 10000, 'isDelete' => false
        ]),
    ]]);
}
var_dump($data);
$client->mutateRows($table, $data, []);

// ############# 使用封装后的
// 批量保存
$t = microtime(true);
$tableObj = new \HelloBase\Table($table, $connection);
$putter = new \HelloBase\Putter($tableObj);
for ($i = 0; $i <= 2; $i++) {
    $rowKey = mt_rand(111222, 999999);
    var_dump($rowKey);
    $putter->pick($rowKey, [
        'info:lon' => mt_rand(1200000, 1394567) / 10000,
        'info:lat' => mt_rand(300000, 400000) / 10000,
    ]);
}

$res = $putter->send();
$end = microtime(true);
var_dump($end - $t);
var_dump($res);

// 单个保存
$data = [
    'info:lon' => mt_rand(1200000, 1394567) / 10000,
    'info:lat' => mt_rand(300000, 400000) / 10000,
];

$res = $tableObj->put(mt_rand(123456, 999999), $data);
var_dump($res);

// var_dump($tableObj->row('199501',['info:count']));
// $res=$tableObj->increment('199501', 'info:count', 1);
// var_dump($res);
// var_dump($tableObj->row('199501',['info:count']));
var_dump('over');
