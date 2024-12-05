package com.example.producer

import com.example.util.FILE_APPEND_PATH
import com.github.javafaker.Faker
import java.io.*
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.collections.HashMap

class FileUtilAppend {

    private val pizzaNames = listOf("Potato Pizza", "Cheese Pizza",
        "Cheese Garlic Pizza", "Super Supreme", "Peperoni")

    private val pizzaShop = listOf("A001", "B001", "C001",
        "D001", "E001", "F001", "G001", "H001", "I001", "J001", "K001", "L001", "M001", "N001",
        "O001", "P001", "Q001")

    private var orderSeq = 5000

    private fun getRandomValueFromList(list: List<String>, random: Random): String {
        val size = list.size
        val index = random.nextInt(size)
        return list[index]
    }

    private fun produceMessage(faker: Faker, random: Random, id: Int): HashMap<String, String> {
        val shopId = getRandomValueFromList(pizzaShop, random)
        val pizzaName = getRandomValueFromList(pizzaNames, random)

        val ordId = "ord$id"
        val customerName = faker.name().fullName()
        val phoneNumber = faker.phoneNumber().phoneNumber()
        val address = faker.address().streetAddress()
        val now = LocalDateTime.now()
        val message = "%s, %s, %s, %s, %s, %s, %s".format(
            ordId,shopId,pizzaName,customerName,phoneNumber,address,now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss",
                Locale.KOREAN))
        )
        val messageMap = HashMap<String, String>()
        messageMap["key"] = shopId
        messageMap["message"] = message
        return messageMap
    }

    fun writeMessage(filePath: String, faker: Faker, random: Random) {
        try {
            val file = File(filePath)
            if (!file.exists()) {
                file.createNewFile()
            }

            val fileWriter = FileWriter(file, true)
            val bufferedWriter = BufferedWriter(fileWriter)
            val printWriter = PrintWriter(bufferedWriter)

            repeat(50) {
                val message = produceMessage(faker, random, orderSeq++)
                printWriter.println("${message["key"]},${message["message"]}")
            }
            printWriter.close()

        } catch (e: IOException) {
            e.printStackTrace()
        }
    }

}

fun main() {
    val fileUtilAppend = FileUtilAppend()

    // seed 값을 고정하여 Random 객체와 Faker 객체를 생성.
    val seed = 2022L
    val random = Random(seed)
    val faker = Faker.instance(random)

    val path = FILE_APPEND_PATH

    // 1000회 반복 수행.
    repeat(1000) { i ->
        // 50 라인의 주문 문자열을 출력
        fileUtilAppend.writeMessage(path, faker, random)
        println("###### iteration: $i file write is done")
        try {
            // 주어진 기간 동안 sleep
            Thread.sleep(20000)
        } catch (e: InterruptedException) {
            e.printStackTrace()
        }
    }
}