import consumer.SimpleConsumer;

public class SimpleConsumerExe {
    public static void main(String[] args) {
        try {
            new SimpleConsumer().init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
