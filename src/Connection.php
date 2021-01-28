<?php

namespace HelloBase;

use Exception;
use Hbase\ColumnDescriptor;
use Hbase\HbaseClient;
use Hbase\IOError;
use HelloBase\Contracts\Connection as ConnectionContract;
use HelloBase\Contracts\Table as TableContract;
use InvalidArgumentException;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Protocol\TBinaryProtocolAccelerated;
use Thrift\Protocol\TCompactProtocol;
use Thrift\Transport\TBufferedTransport;
use Thrift\Transport\TFramedTransport;
use Thrift\Transport\TSocket;
use Thrift\Transport\TSocketPool;
use Thrift\Transport\TTransport;

/**
 * Class Connection
 * @package HelloBase
 */
class Connection implements ConnectionContract
{
    const TRANSPORT_BUFFERED = 'buffered';
    const TRANSPORT_FRAMED   = 'framed';

    const PROTOCOL_BINARY             = 'binary';
    const PROTOCOL_BINARY_ACCELERATED = 'binary_accelerated';
    const PROTOCOL_COMPACT            = 'compact';

    protected $config;

    /**
     * @var TSocket
     */
    protected $socket;

    /**
     * @var TTransport
     */
    protected $transport;

    /**
     * @var TTransport
     */
    protected $protocol;

    /**
     * @var HbaseClient
     */
    protected $client;

    /**
     * @var array
     */
    protected $tables = [];

    /**
     * @var bool
     */
    protected $autoConnect = false;

    /**
     * Connection constructor.
     * @param array $config [
     *                      'host' => '',
     *                      'port' => '',
     *                      'auto_connect' => false,
     *                      'persist' => false,
     *                      'debug_handler' => function ($msg) {
     *
     * },
     * 'send_timeout' => 700000,
     * 'recv_timeout' => 700000,
     * 'transport' => Connection::TRANSPORT_FRAMED,
     * 'protocol' => Connection::PROTOCOL_COMPACT,
     *                      ]
     */
    public function __construct(array $config = [])
    {
        $this->prepareConfig($config);

        if ($this->autoConnect) {
            $this->connect();
        }
    }

    /**
     * 连接hbase
     */
    public function connect()
    {
        $config = $this->config;
        if (is_array($config['host']) && is_array($config['port'])) {
            $this->socket = new TSocketPool($config['host'], $config['port'], $config['persist'], $config['debug_handler']);
        } else {
            $this->socket = new TSocket($config['host'], $config['port'], $config['persist'], $config['debug_handler']);
        }
        $this->socket->setSendTimeout($config['send_timeout']);
        $this->socket->setRecvTimeout($config['recv_timeout']);
        if (!empty($config['debug_handler']) && is_callable($config['debug_handler'])) {
            $this->socket->setDebug(true);#为了打印日志
        }
        switch ($config['transport']) {
            case self::TRANSPORT_BUFFERED:
                $this->transport = new TBufferedTransport($this->socket);
                break;
            case self::TRANSPORT_FRAMED:
                $this->transport = new TFramedTransport($this->socket);
                break;
            default:
                throw new InvalidArgumentException(sprintf(
                    "Invalid transport config '%s'",
                    $config['transport']
                ));
        }

        switch ($config['protocol']) {
            case self::PROTOCOL_BINARY_ACCELERATED:
                $this->protocol = new TBinaryProtocolAccelerated($this->transport);
                break;
            case self::PROTOCOL_BINARY:
                $this->protocol = new TBinaryProtocol($this->transport);
                break;
            case self::PROTOCOL_COMPACT:
                $this->protocol = new TCompactProtocol($this->transport);
                break;
            default:
                throw new InvalidArgumentException(sprintf(
                    "Invalid protocol config: '%s'",
                    $config['protocol']
                ));
        }

        if (!is_null($config['debug_handler']) && is_callable($config['debug_handler'])) {
            $this->client = HelloBaseClient::getClient($this->protocol, $config['debug_handler']);
        } else {
            $this->client = new HbaseClient($this->protocol);
        }

        if ($this->transport->isOpen()) {
            return;
        }

        try {
            $retry = intval($config['connect_retry'] ?? 0);
            for ($i = 0; $i <= $retry; $i++) {
                try {
                    $this->transport->open();
                    break;
                } catch (Exception $e) {
                    if ($i == $retry) {
                        throw $e;
                    }
                }
            }
        } catch (Exception $exception) {
            $this->socket->close();
        }
    }

    /**
     * 关闭连接
     */
    public function close()
    {
        if ($this->transport === null || !$this->transport->isOpen()) {
            return;
        }

        $this->transport->close();
        $this->transport = null;
        $this->socket->close();
    }

    public function table($name): TableContract
    {
        return new Table($name, $this);
    }

    /**
     * get tables
     * @return array
     * @throws IOError
     */
    public function tables(): array
    {
        if ($this->tables) {
            return $this->tables;
        }

        try {
            return $this->tables = $this->client->getTableNames();
        } catch (IOError $error) {
            throw $error;
        }
    }

    /**
     * 创建表
     * @param string $table          表名
     * @param array  $columnFamilies 表的列定义
     * @return bool
     * @throws IOError
     * @throws \Hbase\AlreadyExists
     * @throws \Hbase\IllegalArgument
     */
    public function createTable($table, array $columnFamilies): bool
    {
        $descriptors = [];

        foreach ($columnFamilies as $column) {
            $descriptors[] = new ColumnDescriptor([
                'name' => trim($column, ':') . ':',
            ]);
        }

        $this->client->createTable($table, $descriptors);

        return true;
    }

    /**
     * 删除表 先禁用然后删除
     * @param string $tableName
     * @return bool
     * @throws IOError
     */
    public function deleteTable(string $tableName)
    {
        $this->client->disableTable($tableName);
        $this->client->deleteTable($tableName);
        return true;
    }

    /**
     * @return HbaseClient
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * 设置配置
     * @param array $config 配置数组
     */
    public function prepareConfig(array $config)
    {
        $this->config = array_merge([
            'host' => 'localhost',
            'port' => '9090',
            'auto_connect' => false,
            'persist' => false,
            'debug_handler' => null,
            'send_timeout' => 1000000,
            'recv_timeout' => 1000000,
            'transport' => self::TRANSPORT_BUFFERED,
            'protocol' => self::PROTOCOL_BINARY_ACCELERATED,
        ], $config);
    }

    /**
     * 获取当前配置
     * @return array
     */
    public function getConfig(): array
    {
        return $this->config;
    }

    public function __destruct()
    {
        $this->close();
    }
}
