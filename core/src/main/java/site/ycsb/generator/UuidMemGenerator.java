package site.ycsb.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class UuidMemGenerator extends Generator<String> {

  private final Random rand = new Random();
  private final List<String> history = new ArrayList<>();

  private String lastValue;

  @Override
  public String nextValue() {
    String uuid = UUID.randomUUID().toString();
    lastValue = uuid;
    history.add(uuid);
    return uuid;
  }

  @Override
  public String lastValue() {
    return lastValue;
  }

  public String getHistorical() {
    return history.get(rand.nextInt(history.size()));
  }
}
