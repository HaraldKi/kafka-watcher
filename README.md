kafka-watcher --- a few classes to watch Kafka consumers and the tip of the log
======

Work in progress.

There is not yet a build file or similar. The file
`src/java/de/pifpafpuf/kavi/QueueWatcher.java` contains the important
bits of how to peek into the `__consumer_offsets` topic to see which
consumers are active. No dependencies on Scala, just Java.
