package com.bsolz.learnkafkastreams.config;

import com.bsolz.learnkafkastreams.exceptions.GreetingStreamProcessorExceptionHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

import static com.bsolz.learnkafkastreams.topology.GreetingStreamTopology.GREETINGS;
import static com.bsolz.learnkafkastreams.topology.GreetingStreamTopology.GREETINGS_OUTPUT;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class GreetingStreamConfiguration {

    private final KafkaProperties kafkaProperties;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        var props = kafkaProperties.buildStreamsProperties(null);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, RecoveringDeserializationExceptionHandler.class);
        props.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, recoverer());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanConfigurer() {
        return factoryBean -> {
            factoryBean.setStreamsUncaughtExceptionHandler(new GreetingStreamProcessorExceptionHandler());
        };
    }

    private ConsumerRecordRecoverer recoverer() {
        return (consumerRecord, e) -> log.error("Exception {}, Failed Record {}", e.getMessage(), consumerRecord, e);
    }

    @Bean
    public NewTopic greetingTopic() {
        return TopicBuilder
                .name(GREETINGS)
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic greetingOutputTopic() {
        return TopicBuilder
                .name(GREETINGS_OUTPUT)
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }
}
