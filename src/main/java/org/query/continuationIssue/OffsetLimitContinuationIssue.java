package org.query.continuationIssue;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.implementation.TestConfigurations;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class OffsetLimitContinuationIssue {

    private static final Integer ITEM_COUNT = 300_000;
    private static final String DATABASE_NAME = "testContinuationIssueDb";
    private static final String CONTAINER_NAME = "testContinuationIssueCt";

    private static final LocalDateTime[] POSSIBLE_DATES = new LocalDateTime[10];

    private static final Integer PROVISIONED_THROUGHPUT = 100_000;
    private static final Logger logger = LoggerFactory.getLogger(OffsetLimitContinuationIssue.class);

    public static void main(String[] args) {

        POSSIBLE_DATES[0] = LocalDateTime.of(2010, 6, 1, 0, 0);
        POSSIBLE_DATES[1] = LocalDateTime.of(2020, 6, 1, 0, 0);
        POSSIBLE_DATES[2] = LocalDateTime.of(2030, 6, 1, 0, 0);
        POSSIBLE_DATES[3] = LocalDateTime.of(2040, 6, 1, 0, 0);
        POSSIBLE_DATES[4] = LocalDateTime.of(2050, 6, 1, 0, 0);
        POSSIBLE_DATES[5] = LocalDateTime.of(2060, 6, 1, 0, 0);
        POSSIBLE_DATES[6] = LocalDateTime.of(2070, 6, 1, 0, 0);
        POSSIBLE_DATES[7] = LocalDateTime.of(2080, 6, 1, 0, 0);
        POSSIBLE_DATES[8] = LocalDateTime.of(2090, 6, 1, 0, 0);
        POSSIBLE_DATES[9] = LocalDateTime.of(2100, 6, 1, 0, 0);

        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();

        try (CosmosClient cosmosClient = getCosmosClient())  {

            cosmosClient.createDatabaseIfNotExists(DATABASE_NAME);
            CosmosDatabase testContinuationIssueDb = cosmosClient.getDatabase(DATABASE_NAME);

            testContinuationIssueDb.createContainerIfNotExists(CONTAINER_NAME, "/id", ThroughputProperties.createManualThroughput(PROVISIONED_THROUGHPUT));
            CosmosContainer testContinuationIssueContainer = testContinuationIssueDb.getContainer(CONTAINER_NAME);

            // insertDocuments(testContinuationIssueContainer, threadLocalRandom);

            for (int i = 0; i < 10; i++) {
                runQueryWithContinuation(testContinuationIssueContainer, 1000);
                runOffsetLimitQuery(testContinuationIssueContainer, 1000);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {

        }
    }

    private static void insertDocuments(CosmosContainer cosmosContainer, Random random) {

        for (int i = 0; i < ITEM_COUNT; i++) {

            Entity entity = new Entity();

            String id = UUID.randomUUID().toString();

            DocumentType[] documentTypes = DocumentType.values();

            int chosenDocumentType = random.nextInt(documentTypes.length);

            Status[] statuses = Status.values();

            int chosenStatus = random.nextInt(statuses.length);

            entity.setId(id);
            entity.setDocumentType(documentTypes[chosenDocumentType].name());
            entity.setStatus(statuses[chosenStatus].name());

            int chosenDateIndex = random.nextInt(POSSIBLE_DATES.length);

            LocalDateTime chosenDate = POSSIBLE_DATES[chosenDateIndex];
            entity.setExpectedProcessTime(chosenDate.toString());

            cosmosContainer.upsertItem(entity);

            logger.info("Inserted documents : {}", (i + 1));
        }
    }

    private static void runOffsetLimitQuery(CosmosContainer cosmosContainer, int pageSize) {

        int recordCount = 0;
        int totalRecords = 0;
        int pageCount = 0;

        List<Entity> entities = new ArrayList<>();

        do {

            String query = "SELECT * FROM c WHERE c.expectedProcessTime >= '2019-06-01T00:00' AND c.expectedProcessTime <= '2039-06-01T00:00'" + " OFFSET " + (pageSize * pageCount) + " LIMIT " + pageSize;

//            String query = "SELECT * FROM c" + " OFFSET " + (pageSize * pageCount) + " LIMIT " + pageSize;
            CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
            options.setQueryMetricsEnabled(false);

            CosmosPagedIterable<Entity> iterable = cosmosContainer.queryItems(query, options, Entity.class);

            for (FeedResponse<Entity> feedResponse : iterable.iterableByPage()) {
                entities.addAll(feedResponse.getResults());
            }

            recordCount = entities.size() - totalRecords;
            totalRecords = entities.size();
            pageCount++;
        } while (pageSize != 0 && pageSize <= recordCount);

        logger.info("Total records through OFFSET + LIMIT : {}", totalRecords);
    }

    private static void runQueryWithContinuation(CosmosContainer cosmosContainer, int pageSize) {

        String continuationToken = null;

        List<Entity> entities = new ArrayList<>();
        //String query = "SELECT * FROM c WHERE c.expectedProcessTime >= '2025-02-19T15:40' AND c.expectedProcessTime <= '2026-02-19T15:40'";

        String query = "SELECT * FROM c WHERE c.expectedProcessTime >= '2019-06-01T00:00' AND c.expectedProcessTime <= '2039-06-01T00:00'";
        do {

            CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
            options.setQueryMetricsEnabled(false);

            CosmosPagedIterable<Entity> iterable = cosmosContainer.queryItems(query, options, Entity.class);

            for (FeedResponse<Entity> feedResponse : iterable.iterableByPage(continuationToken, pageSize)) {
                entities.addAll(feedResponse.getResults());

                continuationToken = feedResponse.getContinuationToken();
            }

        } while (continuationToken != null);

        logger.info("Total records through continuation token : {}", entities.size());
    }

    private static CosmosClient getCosmosClient() {

        return new CosmosClientBuilder()
                .endpoint(TestConfigurations.HOST)
                .key(TestConfigurations.MASTER_KEY)
                .buildClient();
    }
}
