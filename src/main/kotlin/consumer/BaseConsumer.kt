package com.example.consumer

import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import util.KAFKA_SERVER_LOCALHOST
import util.KOTLIN_FILE_TOPIC
import util.logger
import java.io.Serializable
import java.time.Duration
import java.util.*

class BaseConsumer<K : Serializable, V : Serializable>(
    consumerProps: Properties,
    val topic: List<String>
) {
    val log = logger()

    private val kafkaConsumer: KafkaConsumer<K, V> = KafkaConsumer<K, V>(consumerProps)

    fun initConsumer() {
        kafkaConsumer.subscribe(topic)
        shutdownHookToRuntime(kafkaConsumer)
    }

    private fun shutdownHookToRuntime(kafkaConsumer: KafkaConsumer<K, V>) {
        val mainThread = Thread.currentThread()

        Runtime.getRuntime().addShutdownHook(Thread {
            log.info("main program starts to exit by calling wakeup")
            kafkaConsumer.wakeup()
            try {
                mainThread.join()
            } catch (ex: InterruptedException) {
                log.error("Interrupted while waking up", ex)
            }
        })
    }

    private fun processRecord(record: ConsumerRecord<K, V>) {
        log.info(
            """
            record key: ${record.key()}, partition: ${record.partition()}, record value: ${record.value()}
        """.trimIndent()
        )
    }

    private fun processRecords(records: ConsumerRecords<K, V>) {
        records.forEach { processRecord(it) }
    }

    fun pollConsumes(durationMillis: Long, commitMode: String) {
        try {
            while (true) {
                if (commitMode.equals("sync")) {
                    pollCommitSync(durationMillis)
                } else {
                    pollCommitAsync(durationMillis)
                }
            }
        } catch (e: WakeupException) {
            log.error("wake up exception",e)
        } catch (e: Exception){
            log.error("exception",e)
        }finally {
            log.info("#### commit sync before closing")
            kafkaConsumer.commitSync()
            log.info("finally consumer is closing")
            closeConsumer()
        }
    }

    private fun pollCommitAsync(durationMillis: Long) {
        val consumerRecords = kafkaConsumer.poll(Duration.ofMillis(durationMillis))
        processRecords(consumerRecords)
        kafkaConsumer.commitAsync { offsets, exception ->
            if (exception != null) {
                log.error("offsets ${offsets} is not completed, error: ${exception.message}")
            }
        }
    }

    private fun pollCommitSync(durationMillis: Long) {
        val consumerRecords = kafkaConsumer.poll(Duration.ofMillis(durationMillis))
        processRecords(consumerRecords)
        try {
            if (consumerRecords.count() > 0) {
                kafkaConsumer.commitSync()
                log.info("commit sync has been called")
            }
        } catch (e: CommitFailedException) {
            log.error("commit error", e)
        }
    }

    fun closeConsumer() = kafkaConsumer.close()

}

fun main() {
    val props = Properties()
    props[BOOTSTRAP_SERVERS_CONFIG] = KAFKA_SERVER_LOCALHOST
    props[KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    props[VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    props[GROUP_ID_CONFIG] = "file-group"
    props[ENABLE_AUTO_COMMIT_CONFIG] = false

    val baseConsumer = BaseConsumer<String,String>(props, listOf(KOTLIN_FILE_TOPIC))
    baseConsumer.initConsumer()
    val commitMode = "async"
    baseConsumer.pollConsumes(100,commitMode)
    baseConsumer.closeConsumer()
}