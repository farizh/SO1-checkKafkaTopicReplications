# SO1-checkKafkaTopicReplications
This script will check the topics and will print out if they have been replicated to every kafka broker 


Pre-requisites:
Python 2.7

Non-dafault Python libraries used:
kazoo 2.5.0
argparse 1.4.0

In case you do not have libraries installed, run following commands:
pip install kazoo
pip install argparse


To run the script:
python kafkaTopicCheck.py --host 'zookeepr address including port' --kafka_topics 'kafka-topics.bat for windows kafka-topics.sh for unix'

Note: if you don't add arguments and just run 'python kafkaTopicCheck.py' default args will be used

Default arguments:
--host 'localhost:2181'
--kafka_topics 'C:/kafka_2.11-2.0.0/bin/windows/kafka-topics.bat' 