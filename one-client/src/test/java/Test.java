import java.io.IOException;

import org.one.remote.cmd.OneConsumer;
import org.tio.utils.json.Json;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 */

/**
 * @author yangkunguo
 *
 */
public class Test {
	public static void main(String []s) {
		
		OneConsumer one=new OneConsumer("123");
		one.setReadSize(10);
		String json=Json.toJson(one);
		System.out.println(json);
		
		OneConsumer no=Json.toBean(json, OneConsumer.class);
		
		System.out.println(no);
		
		 ObjectMapper mapper = new ObjectMapper();
		 try {
			 OneConsumer no2=mapper.readValue(json, OneConsumer.class);
			 System.out.println(no2);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
