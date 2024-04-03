FROM php:8.2-cli AS app

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ENV COMPOSER_ALLOW_SUPERUSER=1
ENV COMPOSER_PROCESS_TIMEOUT 3600
ENV LANG en_US.UTF-8
ENV LC_ALL=C.UTF-8

WORKDIR /code

COPY docker/composer-install.sh /tmp/composer-install.sh
COPY docker/php/php.ini /usr/local/etc/php/php.ini

# Install Dependencies
RUN  apt-get update && apt-get install -y --no-install-recommends \
        wget  \
        curl \
        make \
        git \
        bzip2 \
        time \
        libzip-dev \
        zip \
        unzip \
        openssl \
        vim \
        libpq-dev \
    && rm -r /var/lib/apt/lists/* \
    && chmod +x /tmp/composer-install.sh \
    && /tmp/composer-install.sh

RUN docker-php-source extract \
    && docker-php-ext-configure pgsql -with-pgsql=/usr/local/pgsql \
    && docker-php-ext-install pdo pdo_pgsql pgsql \
    && docker-php-source delete

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/

# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
# copy rest of the app
COPY . /code/
# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD php /code/src/run.php

FROM app AS dev

ARG XDEBUG_VERSION=""

COPY docker/php/xdebug.ini /usr/local/etc/php/conf.d/

RUN pecl install xdebug${XDEBUG_VERSION} \
 && docker-php-ext-enable xdebug