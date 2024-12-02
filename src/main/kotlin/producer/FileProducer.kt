package com.example.producer

import com.example.util.FILE_SAMPLE_PATH
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import util.KAFKA_SERVER_LOCALHOST
import util.KOTLIN_FILE_TOPIC
import util.logger
import java.io.BufferedReader
import java.io.FileReader
import java.util.*

class FileProducer {
    private val log = logger()

    fun run() {
        val props = Properties()
        props[BOOTSTRAP_SERVERS_CONFIG] = KAFKA_SERVER_LOCALHOST
        props[KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

        val producer = KafkaProducer<String, String>(props)
        val path = FILE_SAMPLE_PATH
        sendFileMessages(producer, KOTLIN_FILE_TOPIC, path)

        producer.flush()
        producer.close()
    }

    private fun sendFileMessages(
        producer: KafkaProducer<String, String>,
        topicName: String,
        path: String
    ) {
        val delimiter = ","
        BufferedReader(FileReader(path)).use { bufferedReader ->
            bufferedReader.forEachLine { line ->
                val tokens = line.split(delimiter)
                val key = tokens[0]
                val value = StringBuilder()
                for (i in 1 until tokens.size) {
                    if (1 != tokens.lastIndex) {
                        value.append(tokens[i]).append(delimiter)
                    } else {
                        value.append(tokens[i])
                    }
                }
                sendMessage(producer, topicName, key, value.toString())
            }
        }
    }

    private fun sendMessage(
        producer: KafkaProducer<String, String>,
        topicName: String,
        key: String,
        value: String
    ) {
        ProducerRecord(topicName, key, value).let { record ->
            producer.send(record) { recordMetadata, exception ->
                if (exception != null) {
                    log.error(exception.message)
                }
                log.info(
                    """ ###### record metadata received ######
                        partition: ${recordMetadata.partition()}
                        offset: ${recordMetadata.offset()}
                        timestamp: ${recordMetadata.timestamp()}
                    """.trimIndent()
                )
            }
        }
    }
}

fun main() {
    FileProducer().run()
}