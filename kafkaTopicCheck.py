# Scripted by Fariz Huseynli
#script has been developed in windows machine
# August 20th, 2918
# Feel free to use it :)
# Note: NOT INTENDED FOR PRODUCTION USE


import subprocess
from kazoo.client import KazooClient
import argparse

#take the argument for .py file. In case no argument provided default values will be used
def parser():
    parser = argparse.ArgumentParser(description="We are going to check Topics which are not replicated on every node")
    parser.add_argument('--host', default='localhost:2181')
    parser.add_argument('--kafka_topics',default='C:/kafka_2.11-2.0.0/bin/windows/kafka-topics.bat')
    arguments = parser.parse_args()
    return arguments

class nonReplicatedTopics():
    def __init__(self,args):
        #address for zookeepr
        self.zkAddress = args.host
        #kafka-topics.bat path (for windows machine)
        #kafka-topics.sh path (for Unix)
        self.kafka_list = args.kafka_topics

        # list to store the topics which does not have replicas in every node        
        self.nonReplicatedTopicList = list()


    '''Function will be used to get the list of all topics in every kafka node'''
    def getListOfTopics(self):
        try:
            #take list of kafka topics
            response = subprocess.Popen([self.kafka_list, '--list', '--zookeeper', self.zkAddress], stdout=subprocess.PIPE)
            #trim topics, store them as list
            topicList = response.communicate()[0].split('\r\n')[:-1]
            return topicList
        except:
            print("Could not get the list of topics")
            return False

    '''Function will be used to get the list of all kafka nodes'''
    def getListOfBrokers(self):
        try:
            zk = KazooClient(hosts=self.zkAddress)
            zk.start()
            brokers = zk.get_children('brokers/ids')
            return brokers
        except:
            print("Could not get the list of brokers")
            return False

    '''Function will be used to compare the list of nodes which has replica with list of all nodes'''    
    def evaluateTopics(self):
        # get topic list
        topicList = self.getListOfTopics()
        if not topicList:
            return False
        
        #get broker list
        brokers = self.getListOfBrokers()
        if not brokers:
                return False

        # check every topic
        for topic in topicList:
            # get description for topics
            response = subprocess.Popen([self.kafka_list, '--describe', '--zookeeper', self.zkAddress, '--topic', topic.strip()], stdout=subprocess.PIPE)
            topicDesc = response.communicate()[0][:-1]
            # parse description to get the list of nodes which has replica for topic. including leader
            replicas =  unicode(topicDesc[topicDesc.find('Replicas: ')+len('Replicas: '):topicDesc.rfind('\tIsr:')], 'utf-8').split(',')

            # compare the list of all brokers with the list brokers that has replica for the topic
            diffReplcasAndBrokers = set(brokers)- set(replicas)

            # log the result of comparison
            if len(diffReplcasAndBrokers) != 0:
                print(str(topic.strip()) + " does not have replica in the following brokers: " + str(diffReplcasAndBrokers))
                self.nonReplicatedTopicList.append(topic)
            else:
                print(topic + " has replicas in every kafka broker")

        return self.nonReplicatedTopicList


def main():
    obj = nonReplicatedTopics(parser())

    detectedTopics = obj.evaluateTopics()
    print("Below is the list of topics which does not have replica in every kafka node\n" + str(detectedTopics))


if __name__ == '__main__':
    main()




        