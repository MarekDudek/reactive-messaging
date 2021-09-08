# Running application

## Fat jar

`java -jar -Dspring.profiles.active=jms-sync-sender,tibco ./target/reactive-messaging-0.0.1-SNAPSHOT.jar`

`java -jar -Dspring.profiles.active=jms-async-listener,tibco ./target/reactive-messaging-0.0.1-SNAPSHOT.jar`

### with debug enabled

`java -agentlib:jdwp=transport=dt_socket,server=y,address=9991,suspend=n -jar -Dspring.profiles.active=jms-sync-sender,tibco ./target/reactive-messaging-0.0.1-SNAPSHOT.jar`

`java -agentlib:jdwp=transport=dt_socket,server=y,address=9992,suspend=n -jar -Dspring.profiles.active=jms-async-listener,tibco ./target/reactive-messaging-0.0.1-SNAPSHOT.jar`

## Spring Dev Tools

`mvn spring-boot:run -Dspring-boot.run.profiles=jms-sync-sender,tibco`

`mvn spring-boot:run -Dspring-boot.run.profiles=jms-async-listener,tibco`

### with debug forced

`mvn -P debug,9991 spring-boot:run -Dspring-boot.run.profiles=jms-sync-sender,tibco`

`mvn -P debug,9992 spring-boot:run -Dspring-boot.run.profiles=jms-async-listener,tibco`

# Installing necessary components

## Tibco

We need 8.4 version of Java client (at most) and preferably 6.3.0 version of server

### Server

#### 8.4

#### 8.6

#### Linux

Install RPMs

* TIB_ems_8.6.0_linux_x86_64-server.rpm
* TIB_ems_8.6.0_linux_x86_64-java_client.rpm
* TIB_ems_8.6.0_linux_x86_64-thirdparty.rpm
* TIB_ems_8.6.0_linux_x86_64-c_dev_kit.rpm

#### Windows

* Run installer
* Stop service

  `sc.exe stop tibemsd`

* Check service status

  `sc.exe query tibemsd`

* Delete service

  `emsntsrg.exe /r tibemsd`
  
* Create service

  `emsntsrg.exe /i tibemsd c:\UBS\Dev\programs\ems\8.6\bin\ c:\UBS\Dev\programs\ems\8.6\bin\`

* Start service

  `sc.exe start tibemsd`

### Client

Install jars to local Maven repo

mvn install:install-file \
-Dfile=tibjms.jar \
-DgroupId=com.tibco.ems \
-DartifactId=tibjms \
-Dversion=8.6 \
-Dpackaging=jar \
-DgeneratePom=true
