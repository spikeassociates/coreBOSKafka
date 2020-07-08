package consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RebalanceListner implements ConsumerRebalanceListener {
    private KafkaConsumer kafkaConsumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap();

    public RebalanceListner(KafkaConsumer consumer) {
        this.kafkaConsumer = consumer;
    }

    public void setCurrentOffsets(String topic, int partition, long offset) {
        currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset));
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Following Partitions Assigned ...");
        for (TopicPartition partition: partitions)
            System.out.println(partition.partition() + ",");
        kafkaConsumer.commitSync(currentOffsets);
        currentOffsets.clear();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Following Partitions Revoked");
        for (TopicPartition partition: partitions)
            System.out.println(partition.partition() + ",");

        //kafkaConsumer.commitSync(currentOffsets);
        //currentOffsets.clear();
    }
}
