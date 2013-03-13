package pilot.kafka.sample;

import com.tedwon.kafka.sample.custom.MyCustomConsumer;
import org.junit.Test;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created with IntelliJ IDEA.
 * User: ted
 * Date: 3/12/13
 * Time: 5:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class ConsumerTest {

    private Timer consumerStarterTimer;
    private Timer consumerShutdownTimer;
    private MyCustomConsumer consumer;
    private long timestamp = 1341148570000L;

    @Test
    public void read() throws InterruptedException {




        consumerStarterTimer = new Timer("consumerStarterTimerThread-" + this.getClass().getSimpleName(), true);
        consumerStarterTimer.scheduleAtFixedRate(new TimerTask() {



            public void run() {

                consumer = new MyCustomConsumer("" + timestamp, true);
                consumer.run();

            }
        }, 0L, 1000L);


        Thread.sleep(1000);


        consumerShutdownTimer = new Timer("consumerShutdownTimerThread-" + this.getClass().getSimpleName(), true);
        consumerShutdownTimer.scheduleAtFixedRate(new TimerTask() {

            public void run() {

                consumer.shutdownConsumerConnector();

                timestamp += 1000L;

            }
        }, 0L, 1000L);


        // wait infinitely
        synchronized (ConsumerTest.class) {
            ConsumerTest.class.wait();
        }


    }
}
