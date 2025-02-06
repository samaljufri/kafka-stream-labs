# Kafka Stream Labs

| Projects   | Description      |
|------------|------------------|
| lab01      | Oxygen Sensor    |
| sensor-sim | Sensor Simulator |

## Running labs
### Prerequisites
- Apache Kafka is running at localhost.

### Commands
In terminal, at root folder of this project, to run `lab01` execute the following:
```aiignore
./gradlew stream-lab:bootRun --args="--spring.profiles.active=lab01"
```

