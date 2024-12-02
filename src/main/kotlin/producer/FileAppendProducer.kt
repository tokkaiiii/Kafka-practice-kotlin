package com.example.producer

import com.example.event.FileEventHandler
import com.example.event.FileEventSource
import com.example.event.dto.MessageEvent
import com.example.util.FILE_APPEND
import com.example.util.FILE_SAMPLE
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.common.serialization.StringSerializer
import util.KAFKA_SERVER_LOCALHOST
import util.KOTLIN_FILE_TOPIC
import util.SYNC
import util.logger
import java.util.*

class FileAppendProducer {

    private val log = logger()

    fun run(){

        val props = Properties()
        props[BOOTSTRAP_SERVERS_CONFIG] = KAFKA_SERVER_LOCALHOST
        props[KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

        val producer = KafkaProducer<String, String>(props)
        val file = FILE_APPEND

        val eventHandler = FileEventHandler(producer, KOTLIN_FILE_TOPIC, SYNC)

        val fileEventSource = FileEventSource(1000, file, eventHandler)
        val thread = Thread(fileEventSource)
        thread.start()
        thread.join()

    }
}

fun main() {
    FileAppendProducer().run()
}