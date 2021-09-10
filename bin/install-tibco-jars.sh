#!/usr/bin/env bash
IFS=$'\n\t'
set -euxo pipefail

LIB=$TIBEMS_ROOT/lib
GROUP_ID=com.tibco.ems
VERSION=8.6

for FILE in "$LIB"/*.jar
do
  BASE_NAME=$(basename -- "$FILE")
  ARTIFACT_ID="${BASE_NAME%.*}"
  mvn install:install-file \
   -Dfile="$FILE" \
   -DgroupId=$GROUP_ID -DartifactId="$ARTIFACT_ID" -Dversion=$VERSION \
   -Dpackaging=jar -DgeneratePom=true
done