/**
 * 
 */
package org.one.client.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.swing.plaf.synth.SynthStyleFactory;

import org.one.client.ClientInfo;
import org.one.client.ClientUtil;
import org.one.remote.client.RemotingClientStarter;
import org.one.remote.cmd.Command;
import org.one.remote.cmd.OneConsumer;
import org.one.remote.cmd.OneMessage;
import org.one.remote.common.OneMQException;
import org.one.remote.common.enums.RequestType;
import org.tio.client.intf.ClientAioListener;
import org.tio.core.ChannelContext;
import org.tio.core.intf.Packet;

import com.alibaba.fastjson.JSONArray;

/**
 * @author yangkunguo
 *
 */
public class ConsumerClientAioListener implements ClientAioListener{

	private ConsumerMsgListener consumerMsgListener=null;
	
	private Consumer consumer;
	
	private AtomicLong retryNum= new AtomicLong();
	
	private AtomicBoolean isPull=new AtomicBoolean(false);
	
	public ConsumerClientAioListener(ConsumerMsgListener consumerMsgListener,Consumer consumer) {
		this.consumerMsgListener=consumerMsgListener;
		this.consumer=consumer;
	}
	@Override
	public void onAfterConnected(ChannelContext channelContext, boolean isConnected, boolean isReconnect)
			throws Exception {
		System.out.println(channelContext.toString()+" 服务连接成功状态 :"+ isConnected);
		for(ChannelContext cc:ClientInfo.getRegChannels()) {
			
			if(!cc.getServerNode().toString().equals(channelContext.getServerNode().toString())) {
				if(isConnected && consumer!=null) {
					System.out.println(consumer.toString()+" 监听服务连接成功状态 :"+ consumer.getTopic());
					OneConsumer oneConsumer=new OneConsumer(consumer.getTopic());
					oneConsumer.setReadSize(consumer.getReadSize());
					boolean ok=ClientUtil.send(channelContext,oneConsumer);
					System.out.println("尝试连接Broker--"+ok);
				}else {
					retryNum.incrementAndGet();
					//10次连接失败，则重新注册
					if(retryNum.get()>10) {
						consumer.registerConsumer();
						Thread.sleep(1000*20);
					}
				}
			}
		}
	}

	@Override
	public void onAfterDecoded(ChannelContext channelContext, Packet packet, int packetSize) throws Exception {
		// TODO Auto-generated method stub
//		System.out.println("onAfterDecoded :"+ channelContext.toString());
//		Command cmd=(Command) packet;
//		System.out.println(Json.toJson(cmd));
	}

	@Override
	public void onAfterReceivedBytes(ChannelContext channelContext, int receivedBytes) throws Exception {
		// TODO Auto-generated method stub
//		System.out.println("onAfterReceivedBytes :"+ channelContext.toString());
	}

	@Override
	public void onAfterSent(ChannelContext channelContext, Packet packet, boolean isSentSuccess) throws Exception {
		// TODO Auto-generated method stub
//		System.out.println("onAfterSent :"+ channelContext.toString());
//		Command cmd=(Command) packet;
//		System.out.println(Json.toJson(cmd));
	}

	@Override
	public void onAfterHandled(ChannelContext channelContext, Packet packet, long cost) throws Exception {
		Command cmd=(Command) packet;
//		System.out.println(cmd.getReqType()+"收到返回消息---onAfterHandled :"+ channelContext.toString());
		if(cmd.getReqType()==RequestType.COMSUMER_MSG) {
			//返回消息给消费者
			if(consumerMsgListener!=null) {
				isPull.set(true);
				long sleep=1000;
				if(cmd.getBody()!=null) {
					String json = new String(cmd.getBody(), Command.CHARSET);
					List<OneMessage> msgs=JSONArray.parseArray(json, OneMessage.class);
//					System.out.println(msgs.size()+".............."+consumer.getReadSize());
					if(msgs.size()<consumer.getReadSize()) {
						sleep=1000;
					}else {
						sleep=0;
					}
					List<OneConsumer> cons=new ArrayList<>();
					OneConsumer oneConsumer=null;
					for(OneMessage msg:msgs) {
						oneConsumer=new OneConsumer(consumer.getTopic());
						oneConsumer.setReadSize(consumer.getReadSize());
						oneConsumer.set_id(msg.get_id());
						oneConsumer.setTopic(msg.getTopic());
						cons.add(oneConsumer);
					}
					boolean ask=consumerMsgListener.onMessage(channelContext,msgs);
					boolean ok=ClientUtil.askOK(channelContext,ask, cons);
//					System.out.println("ask--"+ok);
				}else {
					OneConsumer oneConsumer=new OneConsumer(consumer.getTopic());
					oneConsumer.setReadSize(consumer.getReadSize());
//					if(sleep>0) {
//						Thread.sleep(sleep);
//					}
					boolean ok=ClientUtil.send(channelContext,oneConsumer);
//					System.out.println("收到空消息，尝试连接Broker--"+ok);
				}
			}else {
				throw new OneMQException("consumerMsgListener do not null,please new ConsumerMsgListener()");
			}
		}
		isPull.set(false);
//		System.out.println("本次处理消息耗时，单位：毫秒   : "+cost);
	}

	/* 连接关闭前触发本方法
	 */
	@Override
	public void onBeforeClose(ChannelContext channelContext, Throwable throwable, String remark, boolean isRemove)
			throws Exception {
		System.out.println("此连接正在关闭 :"+ channelContext.toString());
		Thread.sleep(1000*10);
		consumer.registerConsumer();
	}
	public AtomicBoolean getIsPull() {
		return isPull;
	}
	
	
}
