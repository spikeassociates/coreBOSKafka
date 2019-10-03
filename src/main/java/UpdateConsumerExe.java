import consumer.UpdateConsumer;

public class UpdateConsumerExe {
    public static void main(String[] args) {
        try {
            new UpdateConsumer().init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
