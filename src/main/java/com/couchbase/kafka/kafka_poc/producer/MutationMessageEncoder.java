package com.couchbase.kafka.kafka_poc.producer;

import org.codehaus.jettison.json.JSONObject;

//Provided by the Kafka connector
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.kafka.DCPEvent;
import com.couchbase.kafka.coder.AbstractEncoder;
import kafka.utils.VerifiableProperties;


public class MutationMessageEncoder extends AbstractEncoder {

  /**
   * Inherited constructor
   * 
   * @param properties 
   */
  public MutationMessageEncoder(final VerifiableProperties properties) {
      super(properties);
  }

  /**
   * To transfer the mutation message into bytes
   * 
   * @param dcpe
   * @return 
   */
  @Override
  public byte[] toBytes(final DCPEvent dcpe) {
      
      MutationMessage cbmsg = (MutationMessage) dcpe.message();
      String data = "{\"key\": " +  JSONObject.quote(cbmsg.key().toString())+ ", \"cas\": " +  cbmsg.cas() + ", \"value\": "+ cbmsg.content().toString(CharsetUtil.UTF_8) + "}";
    //  return cbmsg.content().toString(CharsetUtil.UTF_8).getBytes();
      return data.getBytes(CharsetUtil.UTF_8);
  }    
}
