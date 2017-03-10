package com.couchbase.kafka.kafka_poc.producer;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.couchbase.kafka.CouchbaseKafkaConnector;
import com.couchbase.kafka.DefaultCouchbaseKafkaEnvironment;


public class CouchbaseProducer implements IProducer {

    /**
     * The bucket to connect to
     */
    private final String bucket;
    
    /**
     * The password
     */
    private final String password;
   
    /**
     * One Couchbase cluster host
     */
    private final List<String> nodeList;
    
    /**
     * The zookeeper
     */
    private final String zookeeper;
    
    /**
     * The Kafka topic
     */
    private final String topic;
    
    /**
     * The inner Kafka connector
     */
    private final CouchbaseKafkaConnector innerConnector;
    
    private final Logger logger = Logger.getLogger(this.getClass().getName());
    
    /**
     * The constructor which takes all connection details as arguments
     * 
     * @param couchbaseNodes
     * @param bucket
     * @param password
     * @param zookeeper
     * @param topic 
     */
    public CouchbaseProducer(String couchbaseNodes, String bucket, String password, String zookeeper, String topic) {
    
        this.nodeList = Arrays.asList(couchbaseNodes.split(","));
        this.bucket = bucket;
        this.password = password;
        this.zookeeper = zookeeper;
        this.topic = topic;
        
        
        
        DefaultCouchbaseKafkaEnvironment.Builder envBuilder = (DefaultCouchbaseKafkaEnvironment.Builder) DefaultCouchbaseKafkaEnvironment
                        .builder()
                        .kafkaFilterClass("com.couchbase.kafka.kafka_poc.producer.MutationMessageFilter")
                        .kafkaValueSerializerClass("com.couchbase.kafka.kafka_poc.producer.MutationMessageEncoder")
                        .kafkaTopic(this.topic)
                        .kafkaZookeeperAddress(this.zookeeper)
                        
                        .couchbaseNodes(this.nodeList)
                        .couchbaseBucket(this.bucket)
                        .dcpEnabled(true);
        
        //-- The envrionment is optional
        //Per default com.couchbase.kafka.coder.JsonEncoder is used for the value serialization
        innerConnector = CouchbaseKafkaConnector.create(envBuilder.build());
    }

    public void produce() {
    
        innerConnector.run();
    }
    
    /**
     * We want to start this Producer from the command line
     * 
     * @param args 
     */
    public static void main(String[] args) {
    	
    	Logger log = LogManager.getLogManager().getLogger("");
    	for (Handler h : log.getHandlers()) {
    	    h.setLevel(Level.FINEST);
    	}
        
        if (args != null && args.length == 5)
        {
            String pNodes = args[0];
            String pBucket = args[1];
            String pPassword = args[2];
            String pZookeeper = args[3];
            String pTopic = args[4];
            
            CouchbaseProducer producer = new CouchbaseProducer(pNodes, pBucket, pPassword, pZookeeper, pTopic);
            producer.produce();
        }
        else
        {
            System.out.println("Usage: java com.couchbase.example.kafka.producer.CouchbaseProducer ");
            System.out.println("$couchbaseNode $bucket $password $zookeeper $topic");
        }
    }
    
}