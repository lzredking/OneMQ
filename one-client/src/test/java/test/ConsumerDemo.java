package test;

import java.util.List;

import org.one.client.consumer.Consumer;
import org.one.client.consumer.ConsumerMsgListener;
import org.one.remote.cmd.OneMessage;
import org.tio.core.ChannelContext;

public class ConsumerDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Consumer producer=new Consumer("Consumer-2","topic-1");
		producer.setReadSize(50);
		producer.messageListener(new ConsumerMsgListener() {
			//
			@Override
			public Boolean onMessage(ChannelContext channelContext,List<OneMessage> msgs) {
				if(msgs!=null) {
					System.out.println(msgs.size()+".............."+msgs.toString());
					System.out.println(channelContext.toString());
					return true;
				}
				return false;
			}
		});
		try {
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
