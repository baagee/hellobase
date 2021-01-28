<?php
/**
 * Desc: HBaseClient 增加耗时记录
 * User: baagee
 * Date: 2021/1/25
 * Time: 下午4:53
 */

namespace HelloBase;

use Hbase\HbaseClient;
use Thrift\Protocol\TProtocol;

/**
 * Class HBaseClient
 * @package HelloBase
 */
class HelloBaseClient
{
    /**
     * @var HbaseClient|null
     */
    protected static $client = null;

    protected static $debug_handler = null;

    /**
     * HBaseClient constructor.
     */
    private function __construct()
    {
    }

    /**
     * @param TProtocol $input
     * @param null      $debug_handler
     * @return HbaseClient
     */
    public static function getClient($input, $debug_handler = null)
    {
        if (self::$client == null) {
            self::$client = new \Hbase\HbaseClient($input);
            if (!is_null($debug_handler) && is_callable($debug_handler)) {
                self::$debug_handler = $debug_handler;
            }
        }
        return new self();
    }

    /**
     * @param $method
     * @param $args
     * @return mixed|null
     */
    public function __call($method, $args)
    {
        $s1 = microtime(true);
        try {
            if (method_exists(self::$client, $method)) {
                if (strpos($method, 'send_') === 0 || strpos($method, 'recv_') === 0) {
                    return null;
                }
                return call_user_func([self::$client, $method], ...$args);
            }
        } catch (\Exception $e) {
            throw $e;
        } finally {
            if (is_callable(self::$debug_handler)) {
                $s2 = microtime(true);
                $log = sprintf('%s_FINISH cost=%sms', $method, intval(($s2 - $s1) * 1000));
                call_user_func(self::$debug_handler, $log);
            }
        }
        return null;
    }
}
