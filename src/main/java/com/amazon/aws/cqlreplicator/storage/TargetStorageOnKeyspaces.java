/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.storage;

import com.amazon.aws.cqlreplicator.connector.ConnectionFactory;
import com.amazon.aws.cqlreplicator.models.DeleteTargetOperation;
import com.amazon.aws.cqlreplicator.models.PrimaryKey;
import com.amazon.aws.cqlreplicator.models.QueryStats;
import com.amazon.aws.cqlreplicator.models.StatsMetaData;
import com.amazon.aws.cqlreplicator.util.Utils;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.servererrors.*;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class TargetStorageOnKeyspaces
    extends TargetStorage<Object, List<Row>, BatchStatementBuilder, SimpleStatement> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TargetStorageOnKeyspaces.class);
  private static final Pattern REGEX_PIPE = Pattern.compile("\\|");
  private static CqlSession cqlSession;
  private static PreparedStatement psWriteStats;
  private static PreparedStatement psReadStats;
  private static Retry retry;
  private static Retry.EventPublisher publisher;
  private final Properties config;
  private final PreparedStatement psKeyspaces;

  public TargetStorageOnKeyspaces(Properties properties) {
    var connectionFactory = new ConnectionFactory(properties);
    cqlSession = connectionFactory.buildCqlSession("KeyspacesConnector.conf");
    psWriteStats =
        cqlSession.prepare(
            "update replicator.stats set rows=rows+:value where tile=:tile and keyspacename=:keyspacename and tablename=:tablename and ops=:ops");
    psReadStats =
        cqlSession.prepare(
            "select tile, keyspacename, tablename, ops, rows from replicator.stats where ops=:ops and keyspacename=:keyspacename and tablename=:tablename allow filtering");

    var retryConfig =
        RetryConfig.custom()
            .maxAttempts(
                Integer.parseInt(properties.getProperty("REPLICATE_RETRY_MAXATTEMPTS", "1024")))
            .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofMillis(25), 1.1))
            .retryOnException(
                keyspacesExceptions -> keyspacesExceptions instanceof QueryConsistencyException)
            .retryExceptions(
                WriteFailureException.class,
                WriteTimeoutException.class,
                ServerError.class,
                UnavailableException.class,
                ReadFailureException.class,
                ReadTimeoutException.class,
                CoordinatorException.class,
                AllNodesFailedException.class)
            .failAfterMaxAttempts(true)
            .build();
    var registry = RetryRegistry.of(retryConfig);
    retry = registry.retry("TargetKeyspacesStorage");
    publisher = retry.getEventPublisher();

    this.config = properties;
    this.psKeyspaces = cqlSession.prepare(properties.getProperty("SOURCE_CQL_QUERY"));
  }

  public PreparedStatement getCassandraPreparedStatement() {
    return psKeyspaces;
  }

  @Override
  public void tearDown() {
    cqlSession.close();
  }

  @Override
  public List<Row> execute(BatchStatementBuilder batchableStatement) {
    Supplier<List<Row>> supplier = () -> cqlSession.execute(batchableStatement.build()).all();
    publisher.onError(event -> LOGGER.error("Operation was failed on event {}", event.toString()));
    publisher.onRetry(event -> LOGGER.warn("Operation was retried on event {}", event.toString()));
    return Retry.decorateSupplier(retry, supplier).get();
  }

  public List<Row> extract(BoundStatementBuilder boundStatementBuilder) {
    Supplier<List<Row>> supplier = () -> cqlSession.execute(boundStatementBuilder.build()).all();
    publisher.onError(event -> LOGGER.error("Operation was failed on event {}", event.toString()));
    publisher.onRetry(event -> LOGGER.warn("Operation was retried on event {}", event.toString()));
    return Retry.decorateSupplier(retry, supplier).get();
  }

  public boolean execute(SimpleStatement simpleStatement) {
    AtomicBoolean result = new AtomicBoolean(true);
    Supplier<Row> supplier = () -> cqlSession.execute(simpleStatement).one();
    Retry.decorateSupplier(retry, supplier).get();
    publisher.onError(
        event -> {
          LOGGER.error("Operation was failed on event {}", event.toString());
          result.set(false);
        });
    publisher.onRetry(event -> LOGGER.warn("Operation was retried on event {}", event.toString()));
    // if a request is not succeeded set result to false
    publisher.onSuccess(event -> result.set(true));

    return result.get();
  }

  @Override
  public boolean write(SimpleStatement statement) {
    return execute(statement);
  }

  @Override
  public void writeStats(Object o) {
    var statsMetadata = (StatsMetaData) o;
    var boundStatementBuilder =
        psWriteStats
            .boundStatementBuilder()
            .setInt("tile", statsMetadata.getTile())
            .setString("keyspacename", statsMetadata.getKeyspaceName())
            .setString("tablename", statsMetadata.getTableName())
            .setString("ops", statsMetadata.getOps())
            .setLong("value", statsMetadata.getValue())
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

    var batchableStatements = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    batchableStatements.addStatement(
        boundStatementBuilder.setConsistencyLevel(ConsistencyLevel.QUORUM).build());

    execute(batchableStatements);
  }

  @Override
  public List<Row> readStats(Object o) {
    var queryStats = (QueryStats) o;
    var boundStatementBuilder =
        psReadStats
            .boundStatementBuilder()
            .setString("ops", queryStats.getOps())
            .setString("keyspacename", queryStats.getKeyspaceName())
            .setString("tablename", queryStats.getTableName());
    var batchableStatements = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    batchableStatements.addStatement(
        boundStatementBuilder.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE).build());

    return execute(batchableStatements);
  }

  public boolean delete(
      PrimaryKey primaryKey,
      String[] partitionKeyNames,
      String[] clusteringKeyNames,
      Map<String, LinkedHashMap<String, String>> metadata) {
    var batchableStatements = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    List<String> whereClause = new ArrayList<>();

    var pkValues = REGEX_PIPE.split(primaryKey.getPartitionKeys());
    var ckValues = REGEX_PIPE.split(primaryKey.getClusteringColumns());

    for (var col : partitionKeyNames) {
      whereClause.add(String.format("%s=:%s", col, col));
    }
    for (var col : clusteringKeyNames) {
      whereClause.add(String.format("%s=:%s", col, col));
    }

    var finalWhereClause = String.join(" AND ", whereClause);

    String deleteStatement =
        String.format(
            "DELETE FROM %s.%s WHERE %s",
            config.getProperty("TARGET_KEYSPACE"),
            config.getProperty("TARGET_TABLE"),
            finalWhereClause);

    PreparedStatement psDeleteStatement = cqlSession.prepare(deleteStatement);
    BoundStatementBuilder bsDeleteStatement = psDeleteStatement.boundStatementBuilder();

    int i = 0;
    for (var cl : partitionKeyNames) {
      var type = metadata.get("partition_key").get(cl);
      bsDeleteStatement = Utils.aggregateBuilder(type, cl, pkValues[i], bsDeleteStatement);
      i++;
    }

    int k = 0;
    for (var cl : clusteringKeyNames) {
      var type = metadata.get("clustering").get(cl);
      bsDeleteStatement = Utils.aggregateBuilder(type, cl, ckValues[k], bsDeleteStatement);
      k++;
    }

    batchableStatements.addStatement(
        bsDeleteStatement
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            .setIdempotence(true)
            .build());

    Supplier<Boolean> supplier = () -> cqlSession.execute(batchableStatements.build()).wasApplied();
    publisher.onError(event -> LOGGER.error("Operation was failed on event {}", event.toString()));
    publisher.onRetry(event -> LOGGER.warn("Operation was retried on event {}", event.toString()));
    return Retry.decorateSupplier(retry, supplier).get();
  }

  @Override
  public void delete(Object o) {
    var batchableStatements = BatchStatement.builder(DefaultBatchType.UNLOGGED);

    var deleteTargetOperation = (DeleteTargetOperation) o;
    // Deleting the row in the target table
    List<String> whereClause = new ArrayList<>();
    for (String col : deleteTargetOperation.getNames()) {
      whereClause.add(String.format("%s=:%s", col, col));
    }
    String finalWhereClause = String.join(" AND ", whereClause);

    // Build the DELETE statement
    String deleteStatement =
        String.format(
            "DELETE FROM %s.%s WHERE %s",
            deleteTargetOperation.getKeyspaceName(),
            deleteTargetOperation.getTableName(),
            finalWhereClause);

    PreparedStatement psDeleteTargetData = cqlSession.prepare(deleteStatement);
    BoundStatementBuilder bsDeleteTargetData = psDeleteTargetData.boundStatementBuilder();

    int i = 0;
    for (String cl : deleteTargetOperation.getNames()) {
      String type = deleteTargetOperation.getTypes().get(cl);
      bsDeleteTargetData =
          Utils.aggregateBuilder(
              type, cl, deleteTargetOperation.getValues()[i], bsDeleteTargetData);
      i++;
    }

    batchableStatements.addStatement(
        bsDeleteTargetData
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            .setIdempotence(true)
            .build());

    execute(batchableStatements);
  }
}
