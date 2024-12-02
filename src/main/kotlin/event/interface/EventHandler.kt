package com.example.event.`interface`

import com.example.event.dto.MessageEvent

interface EventHandler {
    fun onMessage(messageEvent: MessageEvent)
}