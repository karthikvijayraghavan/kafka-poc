package com.couchbase.kafka.kafka_poc.producer;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.kafka.DCPEvent;

//Provided by the Couchbase Kafka connector
import com.couchbase.kafka.filter.Filter;


public class MutationMessageFilter implements Filter
{

    public boolean pass(final DCPEvent dcpe) {
      
        //Only handle mutation messages
        return dcpe.message() instanceof MutationMessage;
    }
    
}
