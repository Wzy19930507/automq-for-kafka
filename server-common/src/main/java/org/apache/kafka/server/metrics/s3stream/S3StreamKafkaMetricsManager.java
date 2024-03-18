/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.server.metrics.s3stream;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.wrapper.ConfigListener;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class S3StreamKafkaMetricsManager {
    private volatile static S3StreamKafkaMetricsManager instance = null;

    private final List<ConfigListener> baseAttributesListeners = new ArrayList<>();
    private final MultiAttributes<String> brokerAttributes = new MultiAttributes<>(Attributes.empty(),
            S3StreamKafkaMetricsConstants.LABEL_NODE_ID);
    private final MultiAttributes<String> s3ObjectAttributes = new MultiAttributes<>(Attributes.empty(),
            S3StreamKafkaMetricsConstants.LABEL_OBJECT_STATE);
    private final MultiAttributes<String> fetchLimiterAttributes = new MultiAttributes<>(Attributes.empty(),
            S3StreamKafkaMetricsConstants.LABEL_FETCH_LIMITER_NAME);
    private final MultiAttributes<String> fetchExecutorAttributes = new MultiAttributes<>(Attributes.empty(),
            S3StreamKafkaMetricsConstants.LABEL_FETCH_EXECUTOR_NAME);

    private Supplier<Boolean> isActiveSupplier = () -> false;
    private Supplier<Map<Integer, Long>> autoBalancerMetricsTimeMapSupplier = Collections::emptyMap;
    private Supplier<Map<String, Integer>> s3ObjectCountMapSupplier = Collections::emptyMap;
    private Supplier<Long> s3ObjectSizeSupplier = () -> 0L;
    private Supplier<Map<String, Integer>> streamSetObjectNumSupplier = Collections::emptyMap;
    private Supplier<Integer> streamObjectNumSupplier = () -> 0;
    private Supplier<Map<String, Integer>> fetchLimiterPermitNumSupplier = Collections::emptyMap;
    private Supplier<Map<String, Integer>> fetchPendingTaskNumSupplier = Collections::emptyMap;
    private MetricsConfig metricsConfig = new MetricsConfig(MetricsLevel.INFO, Attributes.empty());
    private Meter meter = null;
    private String prefix = "";

    private S3StreamKafkaMetricsManager() {
        baseAttributesListeners.add(brokerAttributes);
        baseAttributesListeners.add(s3ObjectAttributes);
        baseAttributesListeners.add(fetchLimiterAttributes);
        baseAttributesListeners.add(fetchExecutorAttributes);
    }

    public static S3StreamKafkaMetricsManager getInstance() {
        if (instance == null) {
            synchronized (S3StreamKafkaMetricsManager.class) {
                if (instance == null) {
                    instance = new S3StreamKafkaMetricsManager();
                }
            }
        }
        return instance;
    }

    public void configure(MetricsConfig metricsConfig) {
        synchronized (baseAttributesListeners) {
            this.metricsConfig = metricsConfig;
            for (ConfigListener listener : baseAttributesListeners) {
                listener.onConfigChange(metricsConfig);
            }
        }
    }

    public void registerConfigListener(ConfigListener listener) {
        synchronized (baseAttributesListeners) {
            baseAttributesListeners.add(listener);
        }
    }

    public void initialize(Meter meter, String prefix) {
        this.meter = meter;
        this.prefix = prefix;
        initAutoBalancerMetrics();
        initObjectMetrics();
        initFetchMetrics();
    }

    public Meter meter() {
        return meter;
    }

    public String prefix() {
        return prefix;
    }

    public MetricsConfig metricsConfig() {
        return metricsConfig;
    }

    public boolean isActiveController() {
        return isActiveSupplier.get();
    }

    private void initAutoBalancerMetrics() {
        this.meter.gaugeBuilder(this.prefix + S3StreamKafkaMetricsConstants.AUTO_BALANCER_METRICS_TIME_DELAY_METRIC_NAME)
                .setDescription("The time delay of auto balancer metrics per broker")
                .setUnit("ms")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (shouldRecordMetrics()) {
                        Map<Integer, Long> metricsTimeDelayMap = autoBalancerMetricsTimeMapSupplier.get();
                        for (Map.Entry<Integer, Long> entry : metricsTimeDelayMap.entrySet()) {
                            long timestamp = entry.getValue();
                            long delay = timestamp == 0 ? -1 : System.currentTimeMillis() - timestamp;
                            result.record(delay, brokerAttributes.get(String.valueOf(entry.getKey())));
                        }
                    }
                });
    }

    private void initObjectMetrics() {
        this.meter.gaugeBuilder(this.prefix + S3StreamKafkaMetricsConstants.S3_OBJECT_COUNT_BY_STATE)
                .setDescription("The total count of s3 objects in different states")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (shouldRecordMetrics()) {
                        Map<String, Integer> s3ObjectCountMap = s3ObjectCountMapSupplier.get();
                        for (Map.Entry<String, Integer> entry : s3ObjectCountMap.entrySet()) {
                            result.record(entry.getValue(), s3ObjectAttributes.get(entry.getKey()));
                        }
                    }
                });
        this.meter.gaugeBuilder(this.prefix + S3StreamKafkaMetricsConstants.S3_OBJECT_SIZE)
                .setDescription("The total size of s3 objects in bytes")
                .setUnit("bytes")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (shouldRecordMetrics()) {
                        result.record(s3ObjectSizeSupplier.get(), metricsConfig.getBaseAttributes());
                    }
                });
        this.meter.gaugeBuilder(this.prefix + S3StreamKafkaMetricsConstants.STREAM_SET_OBJECT_NUM)
                .setDescription("The total number of stream set objects")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (shouldRecordMetrics()) {
                        Map<String, Integer> streamSetObjectNumMap = streamSetObjectNumSupplier.get();
                        for (Map.Entry<String, Integer> entry : streamSetObjectNumMap.entrySet()) {
                            result.record(entry.getValue(), brokerAttributes.get(entry.getKey()));
                        }
                    }
                });
        this.meter.gaugeBuilder(this.prefix + S3StreamKafkaMetricsConstants.STREAM_OBJECT_NUM)
                .setDescription("The total number of stream objects")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (shouldRecordMetrics()) {
                        result.record(streamObjectNumSupplier.get(), metricsConfig.getBaseAttributes());
                    }
                });
    }

    private void initFetchMetrics() {
        this.meter.gaugeBuilder(this.prefix + S3StreamKafkaMetricsConstants.FETCH_LIMITER_PERMIT_NUM)
                .setDescription("The number of permits in fetch limiters")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (shouldRecordMetrics()) {
                        Map<String, Integer> fetchLimiterPermitNumMap = fetchLimiterPermitNumSupplier.get();
                        for (Map.Entry<String, Integer> entry : fetchLimiterPermitNumMap.entrySet()) {
                            result.record(entry.getValue(), fetchLimiterAttributes.get(entry.getKey()));
                        }
                    }
                });
        this.meter.gaugeBuilder(this.prefix + S3StreamKafkaMetricsConstants.FETCH_PENDING_TASK_NUM)
                .setDescription("The number of pending tasks in fetch executors")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (shouldRecordMetrics()) {
                        Map<String, Integer> fetchPendingTaskNumMap = fetchPendingTaskNumSupplier.get();
                        for (Map.Entry<String, Integer> entry : fetchPendingTaskNumMap.entrySet()) {
                            result.record(entry.getValue(), fetchExecutorAttributes.get(entry.getKey()));
                        }
                    }
                });
    }

    private boolean shouldRecordMetrics() {
        return MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel()) && isActiveSupplier.get();
    }

    public void setIsActiveSupplier(Supplier<Boolean> isActiveSupplier) {
        this.isActiveSupplier = isActiveSupplier;
    }

    public void setAutoBalancerMetricsTimeMapSupplier(Supplier<Map<Integer, Long>> autoBalancerMetricsTimeMapSupplier) {
        this.autoBalancerMetricsTimeMapSupplier = autoBalancerMetricsTimeMapSupplier;
    }

    public void setS3ObjectCountMapSupplier(Supplier<Map<String, Integer>> s3ObjectCountMapSupplier) {
        this.s3ObjectCountMapSupplier = s3ObjectCountMapSupplier;
    }

    public void setS3ObjectSizeSupplier(Supplier<Long> s3ObjectSizeSupplier) {
        this.s3ObjectSizeSupplier = s3ObjectSizeSupplier;
    }

    public void setStreamSetObjectNumSupplier(Supplier<Map<String, Integer>> streamSetObjectNumSupplier) {
        this.streamSetObjectNumSupplier = streamSetObjectNumSupplier;
    }

    public void setStreamObjectNumSupplier(Supplier<Integer> streamObjectNumSupplier) {
        this.streamObjectNumSupplier = streamObjectNumSupplier;
    }

    public void setFetchLimiterPermitNumSupplier(Supplier<Map<String, Integer>> fetchLimiterPermitNumSupplier) {
        this.fetchLimiterPermitNumSupplier = fetchLimiterPermitNumSupplier;
    }

    public void setFetchPendingTaskNumSupplier(Supplier<Map<String, Integer>> fetchPendingTaskNumSupplier) {
        this.fetchPendingTaskNumSupplier = fetchPendingTaskNumSupplier;
    }
}
