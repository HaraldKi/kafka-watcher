@echo off
java -cp "libs;libs\*;libs\Jetty\*" de.pifpafpuf.kawa.KafkaWatcherServer %* >kafka-watcher.log 2>&1

