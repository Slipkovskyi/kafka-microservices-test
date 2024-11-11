FROM php:8.2-cli

RUN apt-get update && apt-get install -y librdkafka-dev
RUN pecl install rdkafka && docker-php-ext-enable rdkafka

WORKDIR /app
COPY . .

CMD ["php", "consumer.php"]