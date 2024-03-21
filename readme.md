
This project contains Apache Kafka to coreBOS and coreBOS to Apache Kafka consumer and producer code.

It is the base starting point for any project that requires synchronizing information between some source of information contained in an Apache Kafka queue and a coreBOS system or, for sending information contained in a coreBOS application to an Apache Kafka queue.

# Install

To install, you need to use only the `deploy` folder

The configuration file is in the [application.properties](deploy/corebos/application.properties) file

with one folder, the maximum instance is One consumer and one producer.

If more are needed, you can clone this repository again and deploy it in another directory.

# General

This part is mandatory

# coreBOS Test Connection

Is to test the coreBOS connection Sample code in  [corebosConnectTest](deploy/corebosConnectTest.bat)

# Producer

Is to open the Producer connection Sample code in  [producer.bat](deploy/producer.bat)

# Consumer

Is to open the Consumer connection Sample code in  [consumer.bat](deploy/consumer.bat)
