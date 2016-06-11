# kafka-watcher --- watch a Kafka log

## What is it?


Simple web UI to check the state of your Kafka logs.

- which topics with how many partitions?
- grep through message keys
- state of the consumer groups

## How to use it?

On *nix systems, unzip the release zip and run `kafka-watcher -h`. On
windows systems, change to the unzipped folder and run (untested):

```
java -cp 'libs;libs/*;libs/Jetty/*' de.pifpafpuf.kavi.KafkaViewerServer -h
```

## How to compile it yourself?

There is no ivy/mavien integration yet. For the brave, here is the list of libs used:

```
libs/
libs/slf4j-log4j12-1.7.6.jar
libs/kafka-clients-0.9.0.1.jar
libs/log4j-1.2.17.jar
libs/Jetty
libs/Jetty/servlet-api-3.1.jar
libs/Jetty/jetty-rewrite-9.3.6.v20151106.jar
libs/Jetty/jetty-io-9.3.6.v20151106.jar
libs/Jetty/jetty-server-9.3.6.v20151106.jar
libs/Jetty/jetty-http-9.3.6.v20151106.jar
libs/Jetty/jetty-security-9.3.6.v20151106.jar
libs/Jetty/jetty-continuation-9.3.6.v20151106.jar
libs/Jetty/jetty-util-9.3.6.v20151106.jar
libs/Jetty/jetty-servlet-9.3.6.v20151106.jar
libs/commons-cli-1.3.1.jar
libs/htmlJgen-1.4.0.jar
libs/slf4j-api-1.7.6.jar
```

Alternatively check the contents of the `libs` folder in the release zip.

The build.xml makes use of https://github.com/HaraldKi/hkAntLib .
