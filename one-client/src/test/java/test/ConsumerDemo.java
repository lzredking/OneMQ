package test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.one.client.consumer.Consumer;
import org.one.client.consumer.ConsumerMsgListener;
import org.one.remote.cmd.OneMessage;
import org.tio.core.ChannelContext;

public class ConsumerDemo {

//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//
//		Consumer producer=new Consumer("Consumer-2","topic-1");
//		producer.setReadSize(50);
//		producer.messageListener(new ConsumerMsgListener() {
//			//
//			@Override
//			public Boolean onMessage(ChannelContext channelContext,List<OneMessage> msgs) {
//				if(msgs!=null) {
//					System.out.println(msgs.size()+".............."+msgs.toString());
//					System.out.println(channelContext.toString());
//					return true;
//				}
//				return false;
//			}
//		});
//		try {
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
	
	public static void main(String []s) {
		AtomicLong num=new AtomicLong();
		Consumer producer=new Consumer("Consumer-1","topic-1");
		producer.setReadSize(50);
		producer.setRegisterServer("127.0.0.1:6666");
		producer.messageListener(new ConsumerMsgListener() {
			//
			@Override
			public Boolean onMessage(ChannelContext channelContext,List<OneMessage> msgs) {
				if(msgs!=null) {
//					System.out.println(channelContext.toString());
					num.addAndGet(msgs.size());
					System.out.println(msgs.size()+".........消息总数....."+num.get());
					return true;
				}
				return false;
			}
		}).start();
		
	}


}
