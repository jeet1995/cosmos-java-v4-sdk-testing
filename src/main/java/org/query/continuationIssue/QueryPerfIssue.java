package org.query.continuationIssue;

import com.azure.core.credential.TokenCredential;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.implementation.apachecommons.lang.StringUtils;
import com.azure.cosmos.implementation.guava25.base.Strings;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.identity.DefaultAzureCredentialBuilder;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class QueryPerfIssue {

    private static final String AAD_LOGIN_ENDPOINT = System.getProperty("AAD_LOGIN_ENDPOINT",
            StringUtils.defaultString(Strings.emptyToNull(
                    System.getenv().get("AAD_LOGIN_ENDPOINT")), "https://login.microsoftonline.com/"));

    private static final String AAD_MANAGED_IDENTITY_ID = System.getProperty("AAD_MANAGED_IDENTITY_ID",
            StringUtils.defaultString(Strings.emptyToNull(
                    System.getenv().get("AAD_MANAGED_IDENTITY_ID")), ""));

    private static final String AAD_TENANT_ID = System.getProperty("AAD_TENANT_ID",
            StringUtils.defaultString(Strings.emptyToNull(
                    System.getenv().get("AAD_TENANT_ID")), ""));

    private static final TokenCredential CREDENTIAL = new DefaultAzureCredentialBuilder()
            .managedIdentityClientId(AAD_MANAGED_IDENTITY_ID)
            .authorityHost(AAD_LOGIN_ENDPOINT)
            .tenantId(AAD_TENANT_ID)
            .build();

    public static void main(String[] args) {
//
//        String query = "SELECT * FROM c WHERE c.pk IN ('AAPL|20241007|1', 'AAPL|20241007|2', 'AAPL|20241007|3', 'AAPL|20241007|4', 'AAPL|20241007|5', 'AAPL|20241007|6', 'AAPL|20241007|7', 'AAPL|20241007|8') AND c.messageTimestamp >= '1728259200000000000' AND c.messageTimestamp <= '1728345599000000000' ORDER BY c.messageTimestamp DESC";

//        String query = "SELECT * FROM C WHERE C.pk LIKE 'AAPL%' ORDER BY C.messageTimestamp DESC";

        List<SqlParameter> sqlParameters = new ArrayList<>();

        sqlParameters.add(new SqlParameter("@pk1", "AAPL|20241007|1"));
        sqlParameters.add(new SqlParameter("@pk2", "AAPL|20241007|2"));
        sqlParameters.add(new SqlParameter("@pk3", "AAPL|20241007|3"));
        sqlParameters.add(new SqlParameter("@pk4", "AAPL|20241007|4"));
        sqlParameters.add(new SqlParameter("@pk5", "AAPL|20241007|5"));
        sqlParameters.add(new SqlParameter("@pk6", "AAPL|20241007|6"));
        sqlParameters.add(new SqlParameter("@pk7", "AAPL|20241007|7"));
        sqlParameters.add(new SqlParameter("@pk8", "AAPL|20241007|8"));
        sqlParameters.add(new SqlParameter("@pk9", "GOOGL|20241007|1"));
        sqlParameters.add(new SqlParameter("@pk10", "GOOGL|20241007|2"));
        sqlParameters.add(new SqlParameter("@pk11", "GOOGL|20241007|3"));
        sqlParameters.add(new SqlParameter("@pk12", "GOOGL|20241007|4"));
        sqlParameters.add(new SqlParameter("@pk13", "GOOGL|20241007|5"));
        sqlParameters.add(new SqlParameter("@pk14", "GOOGL|20241007|6"));
        sqlParameters.add(new SqlParameter("@pk15", "GOOGL|20241007|7"));
        sqlParameters.add(new SqlParameter("@pk16", "GOOGL|20241007|8"));


        String query = "SELECT * FROM c where c.pk IN (@pk1, @pk2, @pk3, @pk4, @pk5, @pk6, @pk7, @pk8, @pk9, @pk10, @pk11, @pk12, @pk13, @pk14, @pk15, @pk16) and ARRAY_CONTAINS([\"TAS\",\"TAQ\"], c.docType, false) and c.messageTimestamp >= 1728259200000000000 and c.messageTimestamp <= 1728345599000000000 ORDER BY c.messageTimestamp DESC, c.recordkey DESC OFFSET 0 LIMIT 10000";

        SqlQuerySpec querySpec = new SqlQuerySpec(query, sqlParameters);

        CosmosAsyncClient client = new CosmosClientBuilder()
                .endpoint("https://abhm-cosmos-single-write.documents.azure.com:443/")
                .credential(CREDENTIAL)
                .directMode()
                .preferredRegions(Arrays.asList("East US 2"))
                .buildAsyncClient();

        CosmosAsyncContainer container
                = client.getDatabase("ric_database").getContainer("container_20241007y");

        for (int i = 0; i < 100; i++) {

            final int finalI = (i + 1);

            AtomicInteger resultCount = new AtomicInteger(0);
            AtomicReference<String> continuationToken = new AtomicReference<>();

            AtomicReference<Duration> duration = new AtomicReference<>(Duration.ofSeconds(0));

            do {

                AtomicReference<Instant> start = new AtomicReference<>(Instant.now());
                AtomicReference<Instant> end = new AtomicReference<>(Instant.now());
                CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();

                options.setMaxDegreeOfParallelism(3000);
//                options.setMaxBufferedItemCount(10000);

                container.queryItems(querySpec, options, Tick.class)
                        .byPage(continuationToken.get(), 10000)
                        .doOnSubscribe(subscription -> {
                            start.set(Instant.now());
                        })
                        .doOnComplete(() -> {
                            end.set(Instant.now());
                            duration.set(Duration.between(start.get(), end.get()));
                        })
//                        .collectList()
                        .doOnNext(result -> {
                            System.out.println("Page from iteration :" + finalI + result.getContinuationToken());
                        })
                        .doOnNext(result -> {
                            System.out.println("Time now : " + Instant.now());
                            System.out.println("Result count : " + resultCount.get());
                            System.out.println("Diagnostics : " + result.getCosmosDiagnostics());
                            System.out.println("E2E Duration (diagnostics): " + result.getCosmosDiagnostics().getDuration());
                            System.out.println("E2E Duration (diagnosticsContext): " + result.getCosmosDiagnostics().getDiagnosticsContext().getDuration());
                        })
                        .flatMap(tickFeedResponse -> {
                            continuationToken.set(tickFeedResponse.getContinuationToken());
                            resultCount.addAndGet(tickFeedResponse.getResults().size());

                            tickFeedResponse.getCosmosDiagnostics().getDiagnosticsContext().getDuration();

                            return Flux.just(tickFeedResponse.getResults());
                        })
                        .doOnSubscribe(subscription -> {
                            start.set(Instant.now());
                            System.out.println("Start: " + start.get());
                        })
                        .doOnTerminate(() -> {
                            end.set(Instant.now());
                            duration.set(Duration.between(start.get(), end.get()));
                        })
//                        .block();
//                        .doOnComplete(() -> {
//                            end.set(Instant.now());
//                            System.out.println("Page processed in duration: " + Duration.between(start.get(), end.get()).toMillis() + " ms");
//                            duration.set(Duration.between(start.get(), end.get()));
//                            System.out.println("End: " + end.get());
//                        })
                        .blockLast();
            } while (continuationToken.get() != null);

            System.out.println("Total results: " + resultCount.get());
            System.out.println("Total duration: " + duration.get().toMillis() + " ms");

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
