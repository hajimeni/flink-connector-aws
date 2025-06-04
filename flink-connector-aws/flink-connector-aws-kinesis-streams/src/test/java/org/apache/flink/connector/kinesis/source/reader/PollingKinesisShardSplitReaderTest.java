/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.source.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.reader.polling.PollingKinesisShardSplitReader;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.connector.kinesis.source.util.KinesisStreamProxyProvider.TestKinesisStreamProxy;
import org.apache.flink.connector.kinesis.source.util.TestUtil;
import org.apache.flink.metrics.testutils.MetricListener;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.SHARD_GET_RECORDS_INTERVAL;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.SHARD_GET_RECORDS_MAX;
import static org.apache.flink.connector.kinesis.source.util.KinesisStreamProxyProvider.getTestStreamProxy;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.STREAM_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestRecord;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

class PollingKinesisShardSplitReaderTest {
    private PollingKinesisShardSplitReader splitReader;
    private TestKinesisStreamProxy testStreamProxy;
    private MetricListener metricListener;
    private Map<String, KinesisShardMetrics> shardMetricGroupMap;
    private Configuration sourceConfig;
    private static final String TEST_SHARD_ID = TestUtil.generateShardId(1);

    @BeforeEach
    public void init() {
        testStreamProxy = getTestStreamProxy();
        metricListener = new MetricListener();
        shardMetricGroupMap = new ConcurrentHashMap<>();

        sourceConfig = new Configuration();
        sourceConfig.set(SHARD_GET_RECORDS_MAX, 50);
        sourceConfig.set(SHARD_GET_RECORDS_INTERVAL, Duration.ZERO);

        shardMetricGroupMap.put(
                TEST_SHARD_ID,
                new KinesisShardMetrics(
                        TestUtil.getTestSplit(TEST_SHARD_ID), metricListener.getMetricGroup()));
        splitReader =
                new PollingKinesisShardSplitReader(
                        testStreamProxy, shardMetricGroupMap, sourceConfig);
    }

    @Test
    void testNoAssignedSplitsHandledGracefully() throws Exception {
        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();

        assertThat(retrievedRecords.nextRecordFromSplit()).isNull();
        assertThat(retrievedRecords.nextSplit()).isNull();
        assertThat(retrievedRecords.finishedSplits()).isEmpty();
    }

    @Test
    void testAssignedSplitHasNoRecordsHandledGracefully() throws Exception {
        // Given assigned split with no records
        testStreamProxy.addShards(TEST_SHARD_ID);
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(getTestSplit(TEST_SHARD_ID))));

        // When fetching records
        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();

        // Then retrieve no records
        assertThat(retrievedRecords.nextRecordFromSplit()).isNull();
        assertThat(retrievedRecords.nextSplit()).isNull();
        assertThat(retrievedRecords.finishedSplits()).isEmpty();
    }

    @Test
    void testSplitWithExpiredShardHandledAsCompleted() throws Exception {
        // Given assigned split with expired shard
        KinesisShardSplit testSplit = getTestSplit(TEST_SHARD_ID);
        testStreamProxy.addShards(testSplit.getShardId());
        testStreamProxy.setGetRecordsExceptionSupplier(
                () ->
                        ResourceNotFoundException.builder()
                                .message("Shard " + testSplit.getShardId() + " does not exist")
                                .build());
        splitReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(testSplit)));

        // When fetching records
        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();

        // Then retrieve no records and mark split as complete
        assertThat(retrievedRecords.nextRecordFromSplit()).isNull();
        assertThat(retrievedRecords.nextSplit()).isNull();
        assertThat(retrievedRecords.finishedSplits()).containsExactly(testSplit.splitId());
    }

    @Test
    void testSingleAssignedSplitAllConsumed() throws Exception {
        // Given assigned split with records
        testStreamProxy.addShards(TEST_SHARD_ID);
        List<Record> expectedRecords =
                Stream.of(getTestRecord("data-1"), getTestRecord("data-2"), getTestRecord("data-3"))
                        .collect(Collectors.toList());
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN,
                TEST_SHARD_ID,
                Collections.singletonList(expectedRecords.get(0)));
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN,
                TEST_SHARD_ID,
                Collections.singletonList(expectedRecords.get(1)));
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN,
                TEST_SHARD_ID,
                Collections.singletonList(expectedRecords.get(2)));
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(getTestSplit(TEST_SHARD_ID))));

        // When fetching records
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < expectedRecords.size(); i++) {
            RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();
            records.addAll(readAllRecords(retrievedRecords));
        }

        assertThat(records).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }

    @Test
    void testMultipleAssignedSplitsAllConsumed() throws Exception {
        // Given assigned split with records
        testStreamProxy.addShards(TEST_SHARD_ID);
        List<Record> expectedRecords =
                Stream.of(getTestRecord("data-1"), getTestRecord("data-2"), getTestRecord("data-3"))
                        .collect(Collectors.toList());
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN,
                TEST_SHARD_ID,
                Collections.singletonList(expectedRecords.get(0)));
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN,
                TEST_SHARD_ID,
                Collections.singletonList(expectedRecords.get(1)));
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN,
                TEST_SHARD_ID,
                Collections.singletonList(expectedRecords.get(2)));
        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(getTestSplit(TEST_SHARD_ID))));

        // When records are fetched
        List<Record> fetchedRecords = new ArrayList<>();
        for (int i = 0; i < expectedRecords.size(); i++) {
            RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();
            fetchedRecords.addAll(readAllRecords(retrievedRecords));
        }

        // Then all records are fetched
        assertThat(fetchedRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }

    @Test
    void testHandleEmptyCompletedShard() throws Exception {
        // Given assigned split with no records, and the shard is complete
        testStreamProxy.addShards(TEST_SHARD_ID);
        testStreamProxy.addRecords(TestUtil.STREAM_ARN, TEST_SHARD_ID, Collections.emptyList());
        KinesisShardSplit split = getTestSplit(TEST_SHARD_ID);
        splitReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
        testStreamProxy.setShouldCompleteNextShard(true);

        // When fetching records
        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();

        // Returns completed split with no records
        assertThat(retrievedRecords.nextRecordFromSplit()).isNull();
        assertThat(retrievedRecords.nextSplit()).isNull();
        assertThat(retrievedRecords.finishedSplits()).contains(split.splitId());
    }

    @Test
    void testFinishedSplitsReturned() throws Exception {
        // Given assigned split with records from completed shard
        testStreamProxy.addShards(TEST_SHARD_ID);
        List<Record> expectedRecords =
                Stream.of(getTestRecord("data-1"), getTestRecord("data-2"), getTestRecord("data-3"))
                        .collect(Collectors.toList());
        testStreamProxy.addRecords(TestUtil.STREAM_ARN, TEST_SHARD_ID, expectedRecords);
        KinesisShardSplit split = getTestSplit(TEST_SHARD_ID);
        splitReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

        // When fetching records
        List<Record> fetchedRecords = new ArrayList<>();
        testStreamProxy.setShouldCompleteNextShard(true);
        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();

        // Then records can be read successfully, with finishedSplit returned once all records are
        // completed
        for (int i = 0; i < expectedRecords.size(); i++) {
            assertThat(retrievedRecords.nextSplit()).isEqualTo(split.splitId());
            assertThat(retrievedRecords.finishedSplits()).isEmpty();
            fetchedRecords.add(retrievedRecords.nextRecordFromSplit());
        }
        assertThat(retrievedRecords.nextSplit()).isNull();
        assertThat(retrievedRecords.finishedSplits()).contains(split.splitId());
        assertThat(fetchedRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }

    @Test
    void testWakeUpIsNoOp() {
        assertThatNoException().isThrownBy(splitReader::wakeUp);
    }

    @Test
    void testPauseOrResumeSplits() throws Exception {
        testStreamProxy.addShards(TEST_SHARD_ID);
        KinesisShardSplit testSplit = getTestSplit(TEST_SHARD_ID);

        List<Record> expectedRecords =
                Stream.of(getTestRecord("data-1"), getTestRecord("data-2"))
                        .collect(Collectors.toList());
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN,
                TEST_SHARD_ID,
                Collections.singletonList(expectedRecords.get(0)));
        testStreamProxy.addRecords(
                TestUtil.STREAM_ARN,
                TEST_SHARD_ID,
                Collections.singletonList(expectedRecords.get(1)));
        splitReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(testSplit)));

        // read data from split
        RecordsWithSplitIds<Record> records = splitReader.fetch();
        assertThat(readAllRecords(records)).containsExactlyInAnyOrder(expectedRecords.get(0));

        // pause split
        splitReader.pauseOrResumeSplits(
                Collections.singletonList(testSplit), Collections.emptyList());
        records = splitReader.fetch();
        // returns incomplete split with no records
        assertThat(records.finishedSplits()).isEmpty();
        assertThat(records.nextSplit()).isNull();
        assertThat(records.nextRecordFromSplit()).isNull();

        // resume split
        splitReader.pauseOrResumeSplits(
                Collections.emptyList(), Collections.singletonList(testSplit));
        records = splitReader.fetch();
        assertThat(readAllRecords(records)).containsExactlyInAnyOrder(expectedRecords.get(1));
    }

    @Test
    void testPauseOrResumeSplitsOnlyPauseReadsFromSpecifiedSplits() throws Exception {
        KinesisShardSplit testSplit1 = getTestSplit(generateShardId(1));
        KinesisShardSplit testSplit2 = getTestSplit(generateShardId(2));
        KinesisShardSplit testSplit3 = getTestSplit(generateShardId(3));

        shardMetricGroupMap.put(
                testSplit1.splitId(),
                new KinesisShardMetrics(testSplit1, metricListener.getMetricGroup()));
        shardMetricGroupMap.put(
                testSplit2.splitId(),
                new KinesisShardMetrics(testSplit2, metricListener.getMetricGroup()));
        shardMetricGroupMap.put(
                testSplit3.splitId(),
                new KinesisShardMetrics(testSplit3, metricListener.getMetricGroup()));

        testStreamProxy.addShards(testSplit1.splitId(), testSplit2.splitId(), testSplit3.splitId());

        List<Record> recordsFromSplit1 =
                Arrays.asList(getTestRecord("split-1-data-1"), getTestRecord("split-1-data-2"));
        List<Record> recordsFromSplit2 =
                Arrays.asList(getTestRecord("split-2-data-1"), getTestRecord("split-2-data-2"));
        List<Record> recordsFromSplit3 =
                Arrays.asList(getTestRecord("split-3-data-1"), getTestRecord("split-3-data-2"));

        recordsFromSplit1.forEach(
                record ->
                        testStreamProxy.addRecords(
                                STREAM_ARN,
                                testSplit1.getShardId(),
                                Collections.singletonList(record)));
        recordsFromSplit2.forEach(
                record ->
                        testStreamProxy.addRecords(
                                STREAM_ARN,
                                testSplit2.getShardId(),
                                Collections.singletonList(record)));
        recordsFromSplit3.forEach(
                record ->
                        testStreamProxy.addRecords(
                                STREAM_ARN,
                                testSplit3.getShardId(),
                                Collections.singletonList(record)));

        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Arrays.asList(testSplit1, testSplit2, testSplit3)));

        // pause split 1 and split 3
        splitReader.pauseOrResumeSplits(
                Arrays.asList(testSplit1, testSplit3), Collections.emptyList());

        // read data from splits and verify that only records from split 2 were fetched by reader
        List<Record> fetchedRecords = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            RecordsWithSplitIds<Record> records = splitReader.fetch();
            fetchedRecords.addAll(readAllRecords(records));
        }
        assertThat(fetchedRecords).containsExactly(recordsFromSplit2.toArray(new Record[0]));

        // resume split 3
        splitReader.pauseOrResumeSplits(
                Collections.emptyList(), Collections.singletonList(testSplit3));

        // read data from splits and verify that only records from split 3 had been read
        fetchedRecords.clear();
        for (int i = 0; i < 10; i++) {
            RecordsWithSplitIds<Record> records = splitReader.fetch();
            fetchedRecords.addAll(readAllRecords(records));
        }
        assertThat(fetchedRecords).containsExactly(recordsFromSplit3.toArray(new Record[0]));
    }

    @Test
    void testCloseClosesStreamProxy() {
        assertThatNoException().isThrownBy(splitReader::close);
        assertThat(testStreamProxy.isClosed()).isTrue();
    }

    @Test
    void testFetchUpdatesTheMillisBehindLatestMetric() throws IOException {
        KinesisShardSplit split = getTestSplit();
        shardMetricGroupMap.put(
                split.getShardId(),
                new KinesisShardMetrics(split, metricListener.getMetricGroup()));
        TestUtil.assertMillisBehindLatest(split, -1L, metricListener);

        splitReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

        splitReader.fetch();
        TestUtil.assertMillisBehindLatest(
                split, TestUtil.MILLIS_BEHIND_LATEST_TEST_VALUE, metricListener);
    }

    @Test
    void testMaxRecordsToGetParameterPassed() throws IOException {
        int maxRecordsToGet = 2;
        sourceConfig.set(SHARD_GET_RECORDS_MAX, maxRecordsToGet);
        splitReader =
                new PollingKinesisShardSplitReader(
                        testStreamProxy, shardMetricGroupMap, sourceConfig);
        testStreamProxy.addShards(TEST_SHARD_ID);
        List<Record> sentRecords =
                Stream.of(getTestRecord("data-1"), getTestRecord("data-2"), getTestRecord("data-3"))
                        .collect(Collectors.toList());

        testStreamProxy.addRecords(TestUtil.STREAM_ARN, TEST_SHARD_ID, sentRecords);

        splitReader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(getTestSplit(TEST_SHARD_ID))));

        RecordsWithSplitIds<Record> retrievedRecords = splitReader.fetch();
        List<Record> records = new ArrayList<>(readAllRecords(retrievedRecords));

        assertThat(sentRecords.size() > maxRecordsToGet).isTrue();
        assertThat(records.size()).isEqualTo(maxRecordsToGet);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 200, 500})
    void testGetRecordsCallRateRespectingIntervalMillis(long intervalMillis) throws Exception {
        sourceConfig.set(SHARD_GET_RECORDS_INTERVAL, Duration.ofMillis(intervalMillis));

        TestTimingKinesisStreamProxy timingStreamProxy = new TestTimingKinesisStreamProxy();

        PollingKinesisShardSplitReader readerUnderTest =
                new PollingKinesisShardSplitReader(
                        timingStreamProxy, shardMetricGroupMap, sourceConfig);

        String testShardId = generateShardId(1);
        timingStreamProxy.addShards(testShardId);

        List<Record> batch1Records =
                Arrays.asList(getTestRecord("batch1-data-1"), getTestRecord("batch1-data-2"));
        List<Record> batch2Records =
                Arrays.asList(getTestRecord("batch2-data-1"), getTestRecord("batch2-data-2"));
        List<Record> batch3Records =
                Arrays.asList(getTestRecord("batch3-data-1"), getTestRecord("batch3-data-2"));

        timingStreamProxy.addRecords(STREAM_ARN, testShardId, batch1Records);
        timingStreamProxy.addRecords(STREAM_ARN, testShardId, batch2Records);
        timingStreamProxy.addRecords(STREAM_ARN, testShardId, batch3Records);

        readerUnderTest.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(getTestSplit(testShardId))));

        List<List<Record>> allRecords = new ArrayList<>();
        List<Long> callTimestamps = new ArrayList<>();

        await().pollInterval(1, TimeUnit.MILLISECONDS)
                .atMost(1, TimeUnit.DAYS)
                .untilAsserted(
                        () -> {
                            RecordsWithSplitIds<Record> retrievedRecords = readerUnderTest.fetch();
                            List<Record> records = readAllRecords(retrievedRecords);
                            if (!records.isEmpty()) {
                                allRecords.add(records);
                                callTimestamps.add(timingStreamProxy.getLastCallTimestamp());
                            }
                            assertThat(timingStreamProxy.getCallCount()).isEqualTo(3);
                        });

        assertThat(allRecords).hasSize(3);
        List<Long> intervals =
                IntStream.range(0, callTimestamps.size() - 1)
                        .mapToObj(i -> callTimestamps.get(i + 1) - callTimestamps.get(i))
                        .collect(Collectors.toList());
        for (long interval : intervals) {
            assertThat(interval).isGreaterThanOrEqualTo(intervalMillis);
            assertThat(interval).isLessThan(intervalMillis + 100);
        }

        readerUnderTest.close();
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 200, 500})
    void testMultipleShardsRespectingDifferentIntervalMillis(long intervalMillis) throws Exception {
        sourceConfig.set(SHARD_GET_RECORDS_INTERVAL, Duration.ofMillis(intervalMillis));

        // シャードIDの設定
        String shard1Id = generateShardId(1);
        String shard2Id = generateShardId(2);

        // 各シャード用のメトリックを登録
        KinesisShardMetrics shard1Metrics =
                new KinesisShardMetrics(getTestSplit(shard1Id), metricListener.getMetricGroup());
        KinesisShardMetrics shard2Metrics =
                new KinesisShardMetrics(getTestSplit(shard2Id), metricListener.getMetricGroup());

        Map<String, KinesisShardMetrics> multiShardMetricGroupMap = new ConcurrentHashMap<>();
        multiShardMetricGroupMap.put(shard1Id, shard1Metrics);
        multiShardMetricGroupMap.put(shard2Id, shard2Metrics);

        TestMultiShardTimingKinesisStreamProxy multiShardTimingProxy =
                new TestMultiShardTimingKinesisStreamProxy();

        PollingKinesisShardSplitReader readerUnderTest =
                new PollingKinesisShardSplitReader(
                        multiShardTimingProxy, multiShardMetricGroupMap, sourceConfig);

        multiShardTimingProxy.addShards(shard1Id, shard2Id);

        for (int i = 0; i < 3; i++) {
            multiShardTimingProxy.addRecords(
                    STREAM_ARN,
                    shard1Id,
                    Collections.singletonList(getTestRecord("shard1-data-" + i)));

            multiShardTimingProxy.addRecords(
                    STREAM_ARN,
                    shard2Id,
                    Collections.singletonList(getTestRecord("shard2-data-" + i)));
        }

        readerUnderTest.handleSplitsChanges(
                new SplitsAddition<>(
                        Arrays.asList(getTestSplit(shard1Id), getTestSplit(shard2Id))));

        List<Long> shard1Timestamps = new ArrayList<>();
        List<Long> shard2Timestamps = new ArrayList<>();

        await().pollInterval(10, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            RecordsWithSplitIds<Record> retrievedRecords = readerUnderTest.fetch();
                            List<Record> records = readAllRecords(retrievedRecords);

                            Map<String, List<Long>> shardTimestamps =
                                    multiShardTimingProxy.getShardCallTimestamps();

                            if (shardTimestamps.get(shard1Id) != null) {
                                shard1Timestamps.clear();
                                shard1Timestamps.addAll(shardTimestamps.get(shard1Id));
                            }

                            if (shardTimestamps.get(shard2Id) != null) {
                                shard2Timestamps.clear();
                                shard2Timestamps.addAll(shardTimestamps.get(shard2Id));
                            }

                            assertThat(shard1Timestamps.size() >= 3 && shard2Timestamps.size() >= 3)
                                    .isTrue();
                        });

        for (int i = 0; i < shard1Timestamps.size() - 1; i++) {
            long interval = shard1Timestamps.get(i + 1) - shard1Timestamps.get(i);
            assertThat(interval).isGreaterThanOrEqualTo(intervalMillis);
            assertThat(interval).isLessThan(intervalMillis + 100);
        }

        for (int i = 0; i < shard2Timestamps.size() - 1; i++) {
            long interval = shard2Timestamps.get(i + 1) - shard2Timestamps.get(i);
            assertThat(interval).isGreaterThanOrEqualTo(intervalMillis);
            assertThat(interval).isLessThan(intervalMillis + 100);
        }

        readerUnderTest.close();
    }

    private static class TestMultiShardTimingKinesisStreamProxy extends TestKinesisStreamProxy {
        private final Map<String, List<Long>> shardCallTimestamps = new ConcurrentHashMap<>();

        @Override
        public GetRecordsResponse getRecords(
                String streamArn, String shardId, StartingPosition shardIterator, int maxRecords) {
            // シャードごとの呼び出し時間を記録
            shardCallTimestamps
                    .computeIfAbsent(shardId, k -> new ArrayList<>())
                    .add(System.currentTimeMillis());
            return super.getRecords(streamArn, shardId, shardIterator, maxRecords);
        }

        public Map<String, List<Long>> getShardCallTimestamps() {
            return shardCallTimestamps;
        }
    }

    private static class TestTimingKinesisStreamProxy extends TestKinesisStreamProxy {
        private long lastCallTimestamp = 0;
        private int callCount = 0;

        @Override
        public GetRecordsResponse getRecords(
                String streamArn, String shardId, StartingPosition shardIterator, int maxRecords) {
            callCount++;
            lastCallTimestamp = System.currentTimeMillis();
            return super.getRecords(streamArn, shardId, shardIterator, maxRecords);
        }

        public long getLastCallTimestamp() {
            return lastCallTimestamp;
        }

        public int getCallCount() {
            return callCount;
        }

        public void resetCallCount() {
            callCount = 0;
        }
    }

    private List<Record> readAllRecords(RecordsWithSplitIds<Record> recordsWithSplitIds) {
        List<Record> outputRecords = new ArrayList<>();
        Record record;
        do {
            record = recordsWithSplitIds.nextRecordFromSplit();
            if (record != null) {
                outputRecords.add(record);
            }
        } while (record != null);

        return outputRecords;
    }
}
