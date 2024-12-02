package com.example.event

import com.example.event.dto.MessageEvent
import com.example.event.`interface`.EventHandler
import com.example.util.READ
import org.apache.kafka.clients.producer.ProducerRecord
import util.logger
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.lang.Thread.*
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class FileEventSource(
    private val updateInterval: Int,
    private val file: File,
    private val eventHandler: EventHandler
) : Runnable {
    private val log = logger()

    private val keepRunning = AtomicBoolean(true)
    private var filePointer = AtomicLong(0)

    override fun run() {
        while (keepRunning.get()) {
            sleep(updateInterval.toLong())
            val fileLength = file.length()
            when  {
                fileLength < filePointer.get() -> {
                    log.info("file was reset as filePointer is longer than file length")
                    filePointer.set(fileLength)
                }
                fileLength > filePointer.get() -> {
                    readAppendAndSend();
                }
                else -> continue
            }
        }
    }

    @Throws(ExecutionException::class,
            IOException::class,
            InterruptedException::class)
    private fun readAppendAndSend() {
        val raf = RandomAccessFile(file, READ)
        raf.seek(filePointer.get())
        var line: String?
        while (raf.readLine().also { line = it } != null) {
            sendMessage(line!!)
        }
        filePointer = AtomicLong(raf.filePointer)
    }

    private fun sendMessage(line: String) {
        val delimiter = ","
        val tokens = line.split(delimiter)
        val key = tokens[0]
        val value = StringBuilder()
        for (i in 1 until tokens.size) {
            if (i != tokens.size - 1) {
                value.append(tokens[i]).append(delimiter)
            }else{
                value.append(tokens[i])
            }
        }
        val messageEvent = MessageEvent(key,value.toString())
        eventHandler.onMessage(messageEvent)
    }
}