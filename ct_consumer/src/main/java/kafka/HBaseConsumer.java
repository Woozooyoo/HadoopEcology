package kafka;

import hbase.HBaseDAO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtil;

import java.util.Arrays;

/**
 * kafka consumer
 */
public class HBaseConsumer {

	public static void main(String[] args) {
		KafkaConsumer<String, String> consumer = new KafkaConsumer<> (PropertiesUtil.properties);
		consumer.subscribe (Arrays.asList (PropertiesUtil.getProperty ("kafka.topics")));

		HBaseDAO hBaseDAO = new HBaseDAO ();

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll (100);
			for (ConsumerRecord<String, String> cr : records) {
				String oriValue = cr.value ();
				System.out.println (cr.value ());
				hBaseDAO.put (oriValue);
			}
		}
	}
}
