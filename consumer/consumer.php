<?php

$retries = 0;
$maxRetries = 5;

while ($retries < $maxRetries) {
    try {
        $conf = new RdKafka\Conf();
        $conf->set('group.id', 'mygroup');
        $conf->set('metadata.broker.list', 'kafka:29092');
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new RdKafka\KafkaConsumer($conf);
        $consumer->subscribe(['test_topic']);

        echo "Successfully connected to Kafka\n";
        echo "Waiting for messages...\n";

        while (true) {
            $message = $consumer->consume(120*1000);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $currentTime = date('Y-m-d H:i:s');
                    echo "[$currentTime] : " . $message->payload . "\n";
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages; waiting...\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out waiting for message...\n";
                    break;
                default:
                    echo "Error: " . $message->errstr() . "\n";
                    break;
            }
        }
    } catch (Exception $e) {
        $retries++;
        if ($retries >= $maxRetries) {
            die("Failed to connect to Kafka after {$maxRetries} attempts: " . $e->getMessage());
        }
        echo "Failed to connect, retrying in 5 seconds...\n";
        sleep(5);
    }
}