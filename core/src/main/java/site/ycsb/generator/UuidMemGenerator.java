package site.ycsb.generator;

import com.github.f4b6a3.uuid.UuidCreator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class UuidMemGenerator extends Generator<String> {

  private static final AtomicLong counter = new AtomicLong(0);
  private static final Object lock = new Object();

  private final Random rand = new Random();
  private final List<String> history = new ArrayList<>();

  private String lastValue;

  @Override
  public String nextValue() {
    String uuid;
    synchronized (lock) {
      uuid = UuidCreator.getRandomBased().toString() + counter.incrementAndGet();
      lastValue = uuid;
      history.add(uuid);
    }
    return uuid;
  }

  @Override
  public String lastValue() {
    synchronized(this) {
      System.out.println(Thread.currentThread() + " last value called");
      return lastValue;
    }
  }

  public String getHistorical() {
    return history.get(rand.nextInt(history.size()));
  }
}
