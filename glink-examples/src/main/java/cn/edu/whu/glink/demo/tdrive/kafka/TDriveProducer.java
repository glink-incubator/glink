package cn.edu.whu.glink.demo.tdrive.kafka;

import org.apache.commons.cli.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Yu Liebing
 * */
public class TDriveProducer {

  private final Options options = new Options();

  private int threadNum = 8;
  private String tdrivePath;
  private String topic;
  private String bootstrapServer;
  private int sleepNanos = -1;

  public TDriveProducer() {
    options.addOption("threadNum", "tn", true, "thread num");
    options.addOption("tdrivePath", "p", true, "Path of the MERGED T-Drive file");
    options.addOption("topic", "t", true, "kafka topic");
    options.addOption("bootstrapServer", "s", true, "kafka bootstrap server");
    options.addOption("throughput", "tp", true, "throughput to write to kafka");
  }

  public void init(String[] args) throws ParseException {
    CommandLine cliParser = new DefaultParser().parse(options, args);
    threadNum = Integer.parseInt(cliParser.getOptionValue("threadNum"));
    tdrivePath = cliParser.getOptionValue("tdrivePath");
    topic = cliParser.getOptionValue("topic");
    bootstrapServer = cliParser.getOptionValue("bootstrapServer");
    if (cliParser.getOptionValue("throughput") != null) {
      int throughput = Integer.parseInt(cliParser.getOptionValue("throughput"));
      sleepNanos = (int) 1e9 / throughput;
    }
  }

  public void run() throws ExecutionException, InterruptedException, IOException {
    ExecutorService executor = Executors.newFixedThreadPool(threadNum);
    List<BlockingQueue<String>> queues = new ArrayList<>(threadNum);
    List<Future<Integer>> counts = new ArrayList<>(threadNum);
    for (int i = 0; i < threadNum; ++i) {
      queues.add(new LinkedBlockingQueue<>());
      Future<Integer> count = executor.submit(new Producer(queues.get(i), topic, bootstrapServer, i));
      counts.add(count);
    }

    // read the T-Drive merged file
    File file = new File(tdrivePath);
    BufferedReader br = new BufferedReader(new FileReader(file));
    int idx = 0;
    String line;
    while ((line = br.readLine()) != null) {
      // sleep, not accurate, just slow down the write speed for latency test
      if (sleepNanos > 0) {
        Thread.sleep(0, sleepNanos);
      }
      queues.get(idx % queues.size()).offer(line);
      ++idx;
    }

    int totalCount = 0;
    for (int i = 0; i < threadNum; ++i) {
      queues.get(i).offer("null");
      int count = counts.get(i).get();
      totalCount += count;
      System.out.printf("Thread %d produced %d records\n", i, count);
    }
    System.out.printf("Produced total %d records\n", totalCount);
    executor.shutdown();
  }

  private void printUsage() {
    new HelpFormatter().printHelp("TDriveProducer", options);
  }

  public static void main(String[] args) {
    TDriveProducer producer = new TDriveProducer();
    try {
      producer.init(args);
      producer.run();
    } catch (Exception e) {
      System.err.println(e.getLocalizedMessage());
      producer.printUsage();
      System.exit(-1);
    }
  }
}
