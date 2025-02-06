## lab01: Oxygen Sensor

Create a Kafka Stream project that collects signals from oxygen 
sensor that transmits oxygen level (SpO2) every second to `oxygen-level` topic.

This oxygen sensor will be used by divers. 
The Kafka Stream will be used by monitoring personnel on the ground.

Send alert message to `emergency` topic if following happen:
* Oxygen level falls below 90.
* Sensor fails to send message longer than 3 seconds.
