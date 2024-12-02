package util

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor
import org.apache.kafka.clients.consumer.RoundRobinAssignor

const val KAFKA_SERVER_LOCALHOST = "localhost:9092"
const val KOTLIN_SIMPLE_TOPIC = "kotlin-simple-topic"
const val KOTLIN_KEY_TOPIC = "kotlin-key-topic"
const val TOPIC_P3_T1 = "topic-p3-t1"
const val TOPIC_P3_T2 = "topic-p3-t2"
const val KOTLIN_FILE_TOPIC = "kotlin-file-topic"


// PARTITION_ASSIGNMENT_STRATEGY_CONFIG
val ROUND_ROBIN_ASSIGNOR = RoundRobinAssignor::class.java.name
val COOPERATIVE_STICKY_ASSIGNOR = CooperativeStickyAssignor::class.java.name

// 동기
const val SYNC = true
const val ASYNC = false