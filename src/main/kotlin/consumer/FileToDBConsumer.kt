package com.example.consumer

import org.apache.kafka.clients.consumer.CommitFailedException
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
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*

class FileToDBConsumer<K : Serializable, V : Serializable>(
    consumerProps: Properties,
    topics: List<String>,
    private val orderDBHandler: OrderDBHandler
) {
    private val log = logger()
    private val kafkaConsumer = KafkaConsumer<K, V>(consumerProps)

    init {
        kafkaConsumer.subscribe(topics)
        shutdownHookToRuntime(kafkaConsumer)
    }

    private fun shutdownHookToRuntime(kafkaConsumer: KafkaConsumer<K, V>) {
        val mainThread = Thread.currentThread()

        Runtime.getRuntime().addShutdownHook(Thread {
            log.info("main program starts to exit by calling wakeup")
            kafkaConsumer.wakeup()
            try {
                mainThread.join()
            } catch (e: InterruptedException) {
                log.error("wake up: ", e)
            }
        })
    }

    private fun processRecord(record: ConsumerRecord<K, V>) {
        val orderDTO = makeOrderDTO(record)
        orderDBHandler.insertOrder(orderDTO)
    }

    private fun makeOrderDTO(record: ConsumerRecord<K, V>): OrderDTO {
        val messageValue = record.value() as String
        log.info("##### messageValue: $messageValue")
        val tokens = messageValue.split(",")
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        return OrderDTO(
            tokens[0],
            tokens[1],
            tokens[2],
            tokens[3],
            tokens[4],
            tokens[5],
            LocalDateTime.parse(tokens[6].trim(), formatter),
        )
    }

    private fun processRecords(records: ConsumerRecords<K, V>) {
        val orders = makeOrders(records)
        orderDBHandler.insertOrders(orders)
    }

    private fun makeOrders(records: ConsumerRecords<K, V>): List<OrderDTO> {
        val order = ArrayList<OrderDTO>()
        records.forEach { record ->
            val orderDTO = makeOrderDTO(record)
            order.add(orderDTO)
        }
        return order
    }

    fun pollConsumes(durationMillis: Long, commitMode: String) {
        if (commitMode == "sync") {
            pollCommitSync(durationMillis)
        } else {
            pollCommitAsync(durationMillis)
        }
    }

    fun pollCommitAsync(durationMillis: Long) {
        try {
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofMillis(durationMillis))
                log.info("consumerRecords count: ${consumerRecords.count()}")
                if (consumerRecords.count() > 0) {
                    try {
                        processRecords(consumerRecords)
                    } catch (e: Exception) {
                        log.error("error: ", e)
                    }
                    kafkaConsumer.commitAsync { offset, exception ->
                        if (exception != null) {
                            log.error("offsets ${offset} is not completed, error: ", exception)
                        }
                    }
                }
            }
        } catch (e: WakeupException) {
            log.error("wakeup exception has been called")
        } catch (e: Exception) {
            log.error("Exception: ", e)
        } finally {
            log.info("#### commit sync before closing")
            kafkaConsumer.commitSync()
            log.info("finally consumer is closing")
            close()
        }

    }

    private fun pollCommitSync(durationMillis: Long) {
        try {
            while (true) {
                val consumerRecords = kafkaConsumer.poll(Duration.ofMillis(durationMillis))
                processRecords(consumerRecords)
                try {
                    if (consumerRecords.count() > 0) {
                        kafkaConsumer.commitSync()
                        log.info("commit sync has been called")
                    }
                } catch (e: CommitFailedException) {
                    log.error("commit fail", e)
                }

            }
        } catch (e: WakeupException) {
            log.error("wakeup exception has been called")
        } catch (e: Exception) {
            log.error("Exception: ", e)
        } finally {
            log.info("#### commit sync before closing")
            kafkaConsumer.commitSync()
            log.info("finally consumer is closing")
            close()
        }
    }

    fun close() {
        kafkaConsumer.close()
        orderDBHandler.close()
    }
}

fun main() {
    val props = Properties()
    props[BOOTSTRAP_SERVERS_CONFIG] = KAFKA_SERVER_LOCALHOST
    props[KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    props[VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    props[GROUP_ID_CONFIG] = "file-group"
    props[ENABLE_AUTO_COMMIT_CONFIG] = false

    val url = "jdbc:postgresql://localhost:5432/postgres"
    val user = "root"
    val password = "1234"

    val orderDBHandler = OrderDBHandler(url, user, password)
    val fileToDBConsumer =
        FileToDBConsumer<String, String>(props, listOf(KOTLIN_FILE_TOPIC), orderDBHandler)
    val commitMode = "async"
    fileToDBConsumer.pollConsumes(100, commitMode)
    fileToDBConsumer.close()

}