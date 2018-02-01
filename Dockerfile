FROM php:7
MAINTAINER Miroslav Cillik <miro@keboola.com>

# Dependencies
RUN apt-get update
RUN apt-get install -y wget curl make git bzip2 time libzip-dev zip unzip openssl vim libpq-dev
RUN docker-php-source extract \
    && docker-php-ext-configure pgsql -with-pgsql=/usr/local/pgsql \
    && docker-php-ext-install pdo pdo_pgsql pgsql \
    && docker-php-source delete

# Composer
WORKDIR /root
RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer

# Main
ADD . /code
WORKDIR /code
RUN echo "memory_limit = -1" >> /etc/php.ini
RUN composer install --no-interaction

CMD php ./run.php --data=/data

