# To-do list
* Recreate development environment to match prod


Exception in thread "SpringApplicationShutdownHook" java.lang.NoClassDefFoundError: ch/qos/logback/core/util/ContextUtil
at ch.qos.logback.core.FileAppender.stop(FileAppender.java:146)
at ch.qos.logback.core.rolling.RollingFileAppender.stop(RollingFileAppender.java:149)
at ch.qos.logback.core.spi.AppenderAttachableImpl.detachAndStopAllAppenders(AppenderAttachableImpl.java:107)
at ch.qos.logback.classic.Logger.detachAndStopAllAppenders(Logger.java:206)
at ch.qos.logback.classic.Logger.recursiveReset(Logger.java:331)
at ch.qos.logback.classic.LoggerContext.reset(LoggerContext.java:223)
at ch.qos.logback.classic.LoggerContext.stop(LoggerContext.java:348)
at org.springframework.boot.logging.logback.LogbackLoggingSystem.lambda$getShutdownHandler$0(LogbackLoggingSystem.java:284)
at java.base/java.lang.Iterable.forEach(Iterable.java:75)
at org.springframework.boot.SpringApplicationShutdownHook.run(SpringApplicationShutdownHook.java:102)
at java.base/java.lang.Thread.run(Thread.java:834)
Caused by: java.lang.ClassNotFoundException: ch.qos.logback.core.util.ContextUtil
at java.base/java.net.URLClassLoader.findClass(URLClassLoader.java:471)
at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:588)
at org.springframework.boot.loader.LaunchedURLClassLoader.loadClass(LaunchedURLClassLoader.java:151)
at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:521)
... 11 more
