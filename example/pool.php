<?php
/**
 * Desc:
 * User: baagee
 * Date: 2021/1/11
 * Time: 上午10:31
 */


require __DIR__ . '/../vendor/autoload.php';

use Hbase\HbaseClient;
use HelloBase\Connection;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TFramedTransport;

// $socketPool = new \Thrift\Transport\TSocketPool([]);
//配置多地址
$socket = new \Thrift\Transport\TSocketPool(['127.0.0.1', '127.0.0.1'], [9091, 9090], false, null);
$socket->setSendTimeout(700000);
$socket->setRecvTimeout(700000);
$transport = new TFramedTransport($socket);
$protocol = new TCompactProtocol($transport);
$client = new HbaseClient($protocol);
$transport->open();

$res = $client->getColumnDescriptors('emp_travel');
var_dump($res);
