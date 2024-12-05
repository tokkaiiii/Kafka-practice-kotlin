package com.example.consumer

import util.logger
import java.sql.*
import java.time.LocalDateTime

class OrderDBHandler(
    val url: String,
    val user: String,
    val password: String
) {
    val log = logger()

    var connection: Connection? = null
    var insertPrepared: PreparedStatement? = null
    val INSTERT_ORDER_SQL = """
        INSERT INTO public.orders
        (ord_id, shop_id, menu_name, user_name, phone_number, address, order_time)
        values (?, ?, ?, ?, ?, ?, ?)
    """.trimIndent()

    init {
        try {
            connection = DriverManager.getConnection(url, user, password)
            insertPrepared = connection?.prepareStatement(INSTERT_ORDER_SQL) ?: throw SQLException()
        } catch (e: SQLException) {
            log.error("SQLException: ", e)
        }
    }

    fun insertOrder(orderDTO: OrderDTO) {
        try {
            val pstmt = connection?.prepareStatement(INSTERT_ORDER_SQL) ?: throw SQLException()
            pstmt.setString(1, orderDTO.orderId)
            pstmt.setString(2, orderDTO.shopId)
            pstmt.setString(3, orderDTO.menuName);
            pstmt.setString(4, orderDTO.userName);
            pstmt.setString(5, orderDTO.phoneNumber);
            pstmt.setString(6, orderDTO.address);
            pstmt.setTimestamp(7, Timestamp.valueOf(orderDTO.orderTime));
            pstmt.executeUpdate()
        } catch (e: SQLException) {
            log.error("SQLException: ", e)
        }


    }

    fun insertOrders(orders: List<OrderDTO>) {
        try {
            val pstmt = connection?.prepareStatement(INSTERT_ORDER_SQL) ?: throw SQLException()
            orders.forEach {
                pstmt.setString(1, it.orderId)
                pstmt.setString(2, it.shopId)
                pstmt.setString(3, it.menuName)
                pstmt.setString(4, it.userName)
                pstmt.setString(5, it.phoneNumber)
                pstmt.setString(6, it.address)
                pstmt.setTimestamp(7, Timestamp.valueOf(it.orderTime))

                pstmt.addBatch()
            }
            pstmt.executeUpdate()
        } catch (e: SQLException) {
            log.error("SQLException: ", e)

        }
    }

    fun close() {
        try {
            log.info("###### OrderDBHandler is closing")
            insertPrepared?.close()
            connection?.close()
        } catch (e: SQLException) {
            log.error("SQLException: ", e)
        }
    }
}

fun main() {
    val url = "jdbc:postgresql://localhost:5432/postgres"
    val user = "root"
    val password = "1234"
    val orderDBHandler = OrderDBHandler(url, user, password)

    val now = LocalDateTime.now()
    val orderDTO = OrderDTO(
        "ord001", "test_shop", "test_menu",
        "test_user", "test_phone", "test_address",
        now
    )

    orderDBHandler.insertOrder(orderDTO)
    orderDBHandler.close()
}