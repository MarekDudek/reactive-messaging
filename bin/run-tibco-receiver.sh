#!/usr/bin/env bash
IFS=$'\n\t'
set -euxo pipefail

VERSION=0.0.5-SNAPSHOT
java -jar -Dspring.profiles.active=jms-sync-receiver,tibco ./target/reactive-messaging-${VERSION}.jar