/**
 * 
 */
package org.one.broker.handler;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.one.broker.message.CacheMsg;
import org.one.broker.message.ConsumerLog;
import org.one.broker.message.QueneInfo;
import org.one.remote.cmd.Command;
import org.one.remote.cmd.OneConsumer;
import org.one.remote.cmd.OneMessage;
import org.one.remote.common.enums.RequestType;
import org.tio.core.ChannelContext;
import org.tio.core.Tio;
import org.tio.core.intf.Packet;
import org.tio.utils.json.Json;

import com.one.store.BrokerStore;

/**
 * @author yangkunguo
 *
 */
public class ComsumerMsgHandler implements BrokerHandler{

	private BrokerStore brokerStore;
	
	@Override
	public void handler(Packet packet, ChannelContext channelContext) throws Exception {
		Command command = (Command) packet;
        byte[] body = command.getBody();
        if(body!=null) {
        	String json = new String(body, Command.CHARSET);
//        	System.out.println(json);
        	OneConsumer cons=Json.toBean(json, OneConsumer.class);
        	QueneInfo.addTopicChannels(cons.getTopic(), channelContext);
        	//标记已经消费记录
        	send2Comsumer(channelContext,cons);
        }
	}

	/**使用已经连接的通道，返回数据
	 * @param channelContext
	 * @param cons
	 * @throws UnsupportedEncodingException
	 */
	private void send2Comsumer(ChannelContext channelContext,OneConsumer cons) throws UnsupportedEncodingException {
		String consumer=channelContext.getClientNode().toString();
		if(channelContext.getClientNode().getIp().equals("$UNKNOWN")) {
			return;
		}
//		isPush.set(true);
//		System.out.println("消息余数："+CacheMsg.getSize());
    	List<OneMessage> omsg=new ArrayList<>(100);
    	//指定消息ID
    	if(cons.getTopic()!=null && cons.get_id()!=null) {
//    		omsg.add(CacheMsg.getCacheMsg(cons.getTopic(), cons.get_id()));
    	}else {
    		//一条
    		if(cons.getReadSize()==null) {
    			cons.setReadSize(1);
    		}//else 
    		{//多条
//    			List<OneMessage> msgs=CacheMsg.getCacheTopicMsg(cons.getTopic());
    			//内存读取
//    			List<OneMessage> msgs=CacheMsg.getCacheTopicMsg(cons.getTopic(),cons.getReadSize());
    			//文件读取
    			List<OneMessage> msgs=getBrokerStore().readMessage(cons.getTopic(), cons.getReadSize());
    			if(!msgs.isEmpty()) {
    				omsg.addAll(msgs);
    				ConsumerLog.addLogs(consumer, msgs);
//    				CacheMsg.removeCacheMsg(msgs);
    				getBrokerStore().removeMessage(cons.getTopic(), msgs);
//    				synchronized (msgs) {
//    					int size=0;
//    					for(OneMessage msg:msgs) {
//    						
//    						if(size==cons.getReadSize())break;
//    						
////    						msg=this.brokerStore.readMessage(cons.getTopic(), msg.get_id());
////    						System.out.println(msgs.size()+" = "+msg);
//    						if(msg!=null) {
//    							omsg.add(msg);
//    							ConsumerLog.addLogs(consumer, msg);
//    							CacheMsg.removeCacheMsg(msg);
//    							size++;
//    						}
//    					}
//					}
//    				if(msgs.size()<=cons.getSize()) {
//    					omsg.addAll(msgs);
//    				}else {
//    				}
    				
//    				for(int i=0;i<cons.getSize();i++) {
//    					OneMessage msg=msgs.get(i);
//    					omsg.add(msg);
//    					ConsumerLog.addLogs(consumer, msg.getTopic(), msg.get_id());
//    				}
    			}else{
//    				System.out.println("消息余数："+CacheMsg.getSize());
    			}
    		}
    	}
    	
    	
//    	log.info("收到消息请求，返回消息："+CacheMsg.getSize()+"--"+cons.getTopic()+"--"+Json.toJson(omsg));
    	Command resppacket = new Command();
    	resppacket.setReqType(RequestType.COMSUMER_MSG);//消费消息
    	if(!omsg.isEmpty()) {
    		boolean isBlank=true;
    		for(OneMessage msg:omsg) {
    			if(msg!=null)
    				isBlank=false;
    		}
    		if(!isBlank) {
    			String json=Json.toJson(omsg);
//    			System.out.println(CacheMsg.getSize(msg));
    			resppacket.setBody(json.getBytes(Command.CHARSET));
    		}
    	}
    	boolean ok=Tio.send(channelContext, resppacket);
//    	System.out.println(ok);
//    	isPush.set(false);
	}

	public BrokerStore getBrokerStore() {
		return brokerStore;
	}

	public void setBrokerStore(BrokerStore brokerStore) {
		this.brokerStore = brokerStore;
	}
}
