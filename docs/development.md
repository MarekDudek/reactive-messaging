# Fat jar

`java -jar -Dspring.profiles.active=jms-sync-sender,tibco ./target/reactive-messaging-0.0.1-SNAPSHOT.jar`

`java -jar -Dspring.profiles.active=jms-async-listener,tibco ./target/reactive-messaging-0.0.1-SNAPSHOT.jar`

## with debug enabled

`java -agentlib:jdwp=transport=dt_socket,server=y,address=9991,suspend=n -jar -Dspring.profiles.active=jms-sync-sender,tibco ./target/reactive-messaging-0.0.1-SNAPSHOT.jar`

`java -agentlib:jdwp=transport=dt_socket,server=y,address=9992,suspend=n -jar -Dspring.profiles.active=jms-async-listener,tibco ./target/reactive-messaging-0.0.1-SNAPSHOT.jar`

# Spring Dev Tools

`mvn spring-boot:run -Dspring-boot.run.profiles=jms-sync-sender,tibco`

`mvn spring-boot:run -Dspring-boot.run.profiles=jms-async-listener,tibco`

## with debug forced

`mvn -P debug,9991 spring-boot:run -Dspring-boot.run.profiles=jms-sync-sender,tibco`

`mvn -P debug,9992 spring-boot:run -Dspring-boot.run.profiles=jms-async-listener,tibco`
