#!/usr/bin/env bash
IFS=$'\n\t'
set -euxo pipefail

VERSION=0.0.4-SNAPSHOT
java -jar -Dspring.profiles.active=jms-async-listener,ibm-mq ./target/reactive-messaging-${VERSION}.jar