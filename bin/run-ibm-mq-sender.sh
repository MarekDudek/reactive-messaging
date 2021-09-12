#!/usr/bin/env bash
IFS=$'\n\t'
set -euxo pipefail

VERSION=0.0.4-SNAPSHOT
java -jar -Dspring.profiles.active=jms-sync-sender,ibm-mq ./target/reactive-messaging-${VERSION}.jar