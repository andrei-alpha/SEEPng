import java.math.BigInteger;
import java.util.Properties;
import java.util.Random;

import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.LogicalSeepQuery;
import uk.ac.imperial.lsds.seep.api.QueryBuilder;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seepcontrib.kafka.config.KafkaConfig;


public class Base implements QueryComposer {

	private final Properties p;
	
	public Base() {
		p = new Properties();
		p.setProperty(KafkaConfig.KAFKA_SERVER, "localhost:9092");
		p.setProperty(KafkaConfig.ZOOKEEPER_CONNECT, "localhost:2181");
		
		// Generate a unique topic for this task
        Random random = new Random();
        String topic = "seep-" + new BigInteger(50, random).toString(32);
        p.setProperty(KafkaConfig.PRODUCER_CLIENT_ID, topic);
        p.setProperty(KafkaConfig.CONSUMER_GROUP_ID, topic);
        p.setProperty(KafkaConfig.BASE_TOPIC, topic);
	}
	
	@Override
	public LogicalSeepQuery compose() {
		System.out.println("[Base] Start to build query");
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "ts")
		                                           .newField(Type.INT, "key")
												   .newField(Type.STRING, "text").build();
		
		LogicalOperator src = queryAPI.newStatelessSource(new Source(), 0);
		LogicalOperator processor = queryAPI.newStatelessOperator(new Processor(), 1);
		LogicalOperator snk = queryAPI.newStatelessSink(new Sink(), 2);
		
		src.connectTo(processor, 0, schema, new DataStore(DataStoreType.KAFKA, p));
		processor.connectTo(snk, 1, schema, new DataStore(DataStoreType.KAFKA, p));
		
		System.out.println("###### Build query finished");
		return QueryBuilder.build();
	}

}
