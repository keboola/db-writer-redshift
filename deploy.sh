#!/bin/bash
docker login -u="$QUAY_USERNAME" -p="$QUAY_PASSWORD" quay.io
docker tag keboola/db-writer-redshift quay.io/keboola/db-writer-redshift:$TRAVIS_TAG
docker push quay.io/keboola/db-writer-redshift:$TRAVIS_TAG