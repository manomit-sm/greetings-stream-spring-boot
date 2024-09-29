package com.bsolz.learnkafkastreams.topology;

import com.bsolz.learnkafkastreams.domain.Greeting;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class GreetingStreamTopology {

    private final ObjectMapper objectMapper;

    public static final String GREETINGS = "greetings";
    public static final String GREETINGS_OUTPUT = "greetings_output";

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        final KStream<String, Greeting> greetingStreams = streamsBuilder
                .stream(
                        GREETINGS,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Greeting.class, objectMapper))
                );
        final KStream<String, Greeting> modifiedStream = greetingStreams
                .mapValues((readOnlyKey, value) -> new Greeting(value.message().toUpperCase(), value.timeStamp()));

        modifiedStream
                .to(
                        GREETINGS_OUTPUT,
                        Produced.with(Serdes.String(), new JsonSerde<>(Greeting.class, objectMapper))
                );
    }
}
