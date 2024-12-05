package com.example.consumer

import java.time.LocalDateTime

data class OrderDTO(
    val orderId: String,
    val shopId: String,
    val menuName: String,
    val userName: String,
    val phoneNumber: String,
    val address: String,
    val orderTime: LocalDateTime
)