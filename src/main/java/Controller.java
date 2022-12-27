import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Controller implements Runnable{

    private static final Logger log = LogManager.getLogger(Controller.class);
    static ArrayList<Partition> topicpartitions1 = new ArrayList<>();
    static int size=1;

    static double wsla = 5.0;
    static List<Consumer> assignment;

/*    public static void main(String[] args) throws InterruptedException, ExecutionException {

    }*/


    static void QueryingPrometheus() {
        HttpClient client = HttpClient.newHttpClient();
        ////////////////////////////////////////////////////
        List<URI> partitions = new ArrayList<>();
        try {
            partitions = Arrays.asList(
                    new URI(Constants.topic1p0),
                    new URI(Constants.topic1p1),
                    new URI(Constants.topic1p2),
                    new URI(Constants.topic1p3),
                    new URI(Constants.topic1p4)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag = new ArrayList<>();
        try {
            partitionslag = Arrays.asList(
                    new URI(Constants.topic1p0lag),
                    new URI(Constants.topic1p1lag),
                    new URI(Constants.topic1p2lag),
                    new URI(Constants.topic1p3lag),
                    new URI(Constants.topic1p4lag)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        ///////////////////////////////////////////////////
        //launch queries for topic 1 lag and arrival get them from prometheus
        List<CompletableFuture<String>> partitionsfutures = partitions.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        List<CompletableFuture<String>> partitionslagfuture = partitionslag.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition = 0;
        double totalarrivalstopic1 = 0.0;
        double partitionArrivalRate = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures) {
            try {
                partitionArrivalRate = Util.parseJsonArrivalRate(cf.get(), partition);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            topicpartitions1.get(partition).setArrivalRate(partitionArrivalRate);
            totalarrivalstopic1 += partitionArrivalRate;
            partition++;
        }
        log.info("totalArrivalRate for  topic 1 {}", totalarrivalstopic1);
        partition = 0;
        double totallag = 0.0;
        long partitionLag = 0L;
        for (CompletableFuture<String> cf : partitionslagfuture) {
            try {
                partitionLag = Util.parseJsonArrivalLag(cf.get(), partition).longValue();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            topicpartitions1.get(partition).setLag(partitionLag);
            totallag += partitionLag;
            partition++;
        }

        log.info("totalLag for topic 1 {}", totallag);

        for (int i = 0; i <= 4; i++) {
            log.info("partition {} for topic 1 has the following arrival rate {} and lag {}", i, topicpartitions1.get(i).getArrivalRate(),
                    topicpartitions1.get(i).getLag());
        }
        log.info("******************");

        ///////////////////////////////////////////////////////////////////////////////////////////////
    }

    @Override
    public void run() {
        for (int i = 0; i <= 4; i++) {
            topicpartitions1.add(new Partition(i, 0, 0));

        }
        log.info("Warming for 15 seconds.");
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (true) {
            log.info("Querying Prometheus");
            QueryingPrometheus();
            //QueryRate.queryConsumerGroup();
            Scale.scaleAsPerBinPack(size);
            log.info("Sleeping for 5 seconds");
            log.info("========================================");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}