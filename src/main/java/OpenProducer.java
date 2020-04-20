import no_corebos.Producer;

public class OpenProducer {
    public static void main(String[] args) {
        Producer producer = new Producer();
        if (args.length < 3)
            args = new String[3];
        producer.publishMessage(args[0], args[1], args[2]);
        producer.close();
    }
}
