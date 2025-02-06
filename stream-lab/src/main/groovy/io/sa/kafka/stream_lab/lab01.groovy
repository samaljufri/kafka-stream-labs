package io.sa.kafka.stream_lab

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Suppressed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.SessionWindows
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.SessionStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer
import org.springframework.context.annotation.Profile
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.config.KafkaStreamsCustomizer
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.support.serializer.JsonSerde
import org.springframework.stereotype.Component

import java.time.Duration
import java.time.LocalDateTime

/**
 * lab01: Oxygen Monitoring Demo.
 * Create a kafka stream project that listens to oxygen sensor that transmits oxygen level (SpO2) every second.
 * This oxygen sensor will be used by divers. The stream app will be used by monitoring personnel on the ground.
 * Send alert message to `emergency` topic if following happen:
 * * Oxygen level falls below 90.
 * * Sensor fails to send message longer than 3 seconds.
 */

// Data class for lab01
class Lab01 {
    static class TopicList {
        static String OXYGEN_LEVEL_TOPIC = "oxygen-level"
        static String EMERGENCY_TOPIC = "emergency"
    }

    static class SessionData {
        long lastTimestamp
        double lastOxygenLevel
    }

    static class OxygenLevel {
        String sensorId
        double oxygenLevel
        long timestamp
    }

    static class EmergencyEvent {
        String sensorId
        String message
        String dateTime
    }
}

@Configuration
@Profile("lab01")
class KafkaStreamsConfig {
    @Value('${spring.kafka.bootstrap-servers}')
    private String bootstrapServers

    @Value('${lab01.kafka.streams.application-id}')
    private String appId

    @Bean
    static KafkaStreamsCustomizer getKafkaStreamsCustomizer() {
        return (kafkaStreams)
                -> kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                println "Streams now in running state"
                kafkaStreams.metadataForLocalThreads().forEach(tm
                        -> println "${tm.threadName()} assignments ${tm.activeTasks()}")
            }
        })
    }

    @Bean
    static StreamsBuilderFactoryBeanCustomizer kafkaStreamsCustomizer() {
        return (streamsFactoryBean)
                -> streamsFactoryBean.setKafkaStreamsCustomizer(getKafkaStreamsCustomizer())
    }

    @Bean
    static KafkaAdmin.NewTopics createTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(Lab01.TopicList.OXYGEN_LEVEL_TOPIC).build(),
                TopicBuilder.name(Lab01.TopicList.EMERGENCY_TOPIC).build(),
        )
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> streamsConfigMap = new HashMap<>()
        streamsConfigMap.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
        streamsConfigMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        return new KafkaStreamsConfiguration(streamsConfigMap)
    }
}

@Component
@Profile("lab01")
class SensorMonitor {
    private final KafkaStreamsConfiguration streamsConfigs

    @Value('${lab01.oxygen.threshold}')
    private double oxygenThreshold

    @Value('${lab01.sensor.timeout.seconds}')
    private int sensorTimeoutSeconds

    SensorMonitor(KafkaStreamsConfiguration streamsConfigs) {
        this.streamsConfigs = streamsConfigs
    }

    Topology topology() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder()
        final JsonSerde<Lab01.OxygenLevel> oxygenLevelSerde = new JsonSerde<>(Lab01.OxygenLevel.class)
        final JsonSerde<Lab01.EmergencyEvent> emergencyEventSerde = new JsonSerde<>(Lab01.EmergencyEvent.class)

        KStream<String, Lab01.OxygenLevel> oxygenStream = streamsBuilder
                .stream(Lab01.TopicList.OXYGEN_LEVEL_TOPIC, Consumed.with(Serdes.String(), oxygenLevelSerde))

        oxygenStream
                .filter { key, value -> value.getOxygenLevel() < oxygenThreshold }
                .map { key, value ->
                    KeyValue.pair(
                            value.getSensorId(),
                            new Lab01.EmergencyEvent(
                                    sensorId: value.getSensorId(),
                                    message: "Oxygen level critical: " + value.getOxygenLevel(),
                                    dateTime: LocalDateTime.now().toString()
                            ),

                    )
                }
                .to(Lab01.TopicList.EMERGENCY_TOPIC, Produced.with(Serdes.String(), emergencyEventSerde))

        oxygenStream
                .groupBy((key, value) ->
                        value.getSensorId(), Grouped.with(Serdes.String(), oxygenLevelSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(sensorTimeoutSeconds)))
                .aggregate(
                        () -> new Lab01.SessionData(lastTimestamp: 0L, lastOxygenLevel: 0.0),
                        (sensorId, value, aggregate) -> {
                            aggregate.setLastTimestamp(value.getTimestamp())
                            aggregate.setLastOxygenLevel(value.getOxygenLevel())
                            return aggregate
                        },
                        (aggKey, agg1, agg2)
                                -> agg1.getLastTimestamp() > agg2.getLastTimestamp() ? agg1 : agg2,
                        Materialized.<String, Lab01.SessionData, SessionStore> as("session-data-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(Lab01.SessionData.class))
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, sessionData) -> KeyValue.pair(
                        windowedKey.key(),
                        new Lab01.EmergencyEvent(
                                sensorId: windowedKey.key(),
                                message: "Sensor inactive for $sensorTimeoutSeconds seconds",
                                dateTime: LocalDateTime.now().toString()
                        )
                ))
                .to(Lab01.TopicList.EMERGENCY_TOPIC, Produced.with(Serdes.String(), emergencyEventSerde))
        Topology topology = streamsBuilder.build(streamsConfigs.asProperties())
        println topology.describe()
        return topology
    }
}

@Component
@Profile("lab01")
class StreamsContainer {
    @Autowired
    private SensorMonitor sensorMonitorTopology
    @Autowired
    private KafkaStreamsConfiguration kafkaStreamsConfiguration

    private KafkaStreams kafkaStreams

    @Bean
    KafkaStreams kafkaStreams() {
        return kafkaStreams
    }

    @PostConstruct
    void init() {
        Topology topology = sensorMonitorTopology.topology()
        kafkaStreams = new KafkaStreams(topology, kafkaStreamsConfiguration.asProperties())
        kafkaStreams.setStateListener { newState, oldState ->
            if (newState == KafkaStreams.State.RUNNING) {
                println "Kafka Streams is running..."
                kafkaStreams.metadataForLocalThreads().forEach { tm ->
                    println "ThreadName: ${tm.threadName()}, ActiveTasks: ${tm.activeTasks()}"
                }
            }
        }
        kafkaStreams.start()
    }

    @PreDestroy
    void tearDown() {
        kafkaStreams.close(Duration.ofSeconds(10))
    }
}