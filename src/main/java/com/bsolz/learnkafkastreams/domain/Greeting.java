package com.bsolz.learnkafkastreams.domain;

import java.time.LocalDateTime;

public record Greeting(String message, LocalDateTime timeStamp) {
}
