#!/usr/bin/env bash
IFS=$'\n\t'
set -euxo pipefail

VERSION=0.0.3-SNAPSHOT
java -jar -Dspring.profiles.active=jms-async-listener,tibco ./target/reactive-messaging-${VERSION}.jar