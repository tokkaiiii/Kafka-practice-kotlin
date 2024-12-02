package com.example.event

import com.example.event.dto.MessageEvent
import com.example.event.`interface`.EventHandler
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import util.logger

class FileEventHandler(
    private val producer: KafkaProducer<String, String> ,
    private val topicName: String,
    private val sync: Boolean
) : EventHandler {

    private val log = logger()



    override fun onMessage(messageEvent: MessageEvent) {
        val record = ProducerRecord(topicName,messageEvent.key,messageEvent.value)
        if (sync){
            val recordMetadata = producer.send(record).get()
            log.info(getRecordMetadata(recordMetadata))
        }else{
            producer.send(record){
                recordMetadata, exception ->
                if (exception != null){
                    log.error("error in sending record", exception)
                }
                    log.info(getRecordMetadata(recordMetadata))

            }
        }
    }

    private fun getRecordMetadata(recordMetadata: RecordMetadata) =
        """###### record metadata received ######
            partition: ${recordMetadata.partition()}
            offset: ${recordMetadata.offset()}
            timestamp: ${recordMetadata.timestamp()}
        """.trimIndent()
}