<?php

$retries = 0;
$maxRetries = 5;

function initKafka() {
    global $retries, $maxRetries;

    while ($retries < $maxRetries) {
        try {
            $conf = new RdKafka\Conf();
            $conf->set('metadata.broker.list', 'kafka:29092');
            $producer = new RdKafka\Producer($conf);
            $producer->addBrokers("kafka:29092");

            // Попытка получить метаданные для проверки подключения
            $topic = $producer->newTopic("test_topic");
            return $producer;
        } catch (Exception $e) {
            $retries++;
            if ($retries >= $maxRetries) {
                throw new Exception("Failed to connect to Kafka after {$maxRetries} attempts");
            }
            sleep(5); // Ждем 5 секунд перед следующей попыткой
        }
    }
}

if (php_sapi_name() === 'cli-server') {
    try {
        if (!isset($_GET['msg'])) {
            http_response_code(400);
            echo json_encode(['error' => 'Missing msg parameter']);
            exit;
        }

        $message = $_GET['msg'];
        $producer = initKafka();
        $topic = $producer->newTopic("test_topic");

        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message);
        $producer->flush(1000);

        echo json_encode(['status' => 'success', 'message' => $message]);
    } catch (Exception $e) {
        http_response_code(500);
        echo json_encode(['error' => $e->getMessage()]);
    }
    exit;
}
