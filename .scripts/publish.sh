#!/bin/bash

if [ "$TRAVIS_REPO_SLUG" == "wolfninja/KeyStore-DynamoDB" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_BRANCH" == "master" ] || [ "$TRAVIS_BRANCH" == "develop" ]; then
  if [[ $(./gradlew -q getVersion) != *SNAPSHOT* ]]; then
      echo 'Snapshots only!'
      return 0
  fi

  echo -e "Starting publish...\n"

  ./gradlew uploadArchives
  RETVAL=$?

  if [ $RETVAL -eq 0 ]; then
    echo 'Completed publish!'
  else
    echo 'Publish failed.'
    return 1
  fi

fi
