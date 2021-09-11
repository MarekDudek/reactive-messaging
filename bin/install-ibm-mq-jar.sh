#!/usr/bin/env bash
IFS=$'\n\t'
set -euxo pipefail

mvn install:install-file -Dfile=com.ibm.mq.allclient-9.2.3.0.jar -DgroupId=com.ibm.mq -DartifactId=allclient-9.2.3.0.jar -Dversion=9.2.3.0 -Dpackaging=jar
