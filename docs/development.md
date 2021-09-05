

java myapp -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=4000, suspend=n


java -jar -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=9990, suspend=n -Dspring.profiles.active=jms-async-listener,tibco ./target/reactive-messaging-0.0.1-SNAPSHOT.jar


java -agentlib:jdwp=transport=dt_socket,server=y,address=9990,suspend=n -jar -Dspring.profiles.active=jms-async-listener,tibco ./target/reactive-messaging-0.0.1-SNAPSHOT.jar

# java fat jar
java -agentlib:jdwp=transport=dt_socket,server=y,address=9990,suspend=n -jar -Dspring.profiles.active=jms-async-listener,tibco ./target/reactive-messaging-0.0.1-SNAPSHOT.jar

# Spring Boot DevTool
# listener
mvn -agentlib:jdwp=transport=dt_socket,server=y,address=9990,suspend=n spring-boot:run -Dspring-boot.run.profiles=jms-async-listener,tibco

# sender
mvn -agentlib:jdwp=transport=dt_socket,server=y,address=9990,suspend=n spring-boot:run -Dspring-boot.run.profiles=jms-sync-sender,tibco
mvn -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=9990,suspend=n spring-boot:run -Dspring-boot.run.profiles=jms-sync-sender,tibco

mvn spring-boot:run -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=9990,suspend=n -Dspring-boot.run.profiles=jms-sync-sender,tibco


mvn spring-boot:run -Dspring-boot.run.profiles=jms-sync-sender,tibco

# Debugging application

# Sender
mvn -P reader-debug spring-boot:run -Dspring-boot.run.profiles=jms-sync-sender,tibco


# Sender
mvn -P debug,9991 spring-boot:run -Dspring-boot.run.profiles=jms-sync-sender,tibco

# Receiver
mvn -P debug,9992 spring-boot:run -Dspring-boot.run.profiles=jms-async-listener,tibco