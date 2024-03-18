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

package kafka.autobalancer.goals;

import com.automq.stream.s3.metrics.MetricsLevel;
import kafka.autobalancer.common.Resource;
import kafka.autobalancer.model.BrokerUpdater;
import org.apache.kafka.server.metrics.s3stream.MultiAttributes;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsConstants;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager;

import java.util.Comparator;
import java.util.Set;

public abstract class AbstractResourceUsageDistributionGoal extends AbstractResourceDistributionGoal {
    private static final double DEFAULT_MAX_LOAD_BYTES = 100 * 1024 * 1024;
    private final Comparator<BrokerUpdater.Broker> highLoadComparator = Comparator.comparingDouble(b -> -b.load(resource()));
    private final Comparator<BrokerUpdater.Broker> lowLoadComparator = Comparator.comparingDouble(b -> b.load(resource()));
    private final MultiAttributes<String> usageBoundAttributes;

    protected long usageDetectThreshold;
    protected double usageAvgDeviation;
    private double usageAvg;
    private double usageDistLowerBound;
    private double usageDistUpperBound;

    public AbstractResourceUsageDistributionGoal() {
        this.usageBoundAttributes = new MultiAttributes<>(S3StreamKafkaMetricsManager.getInstance().metricsConfig().getBaseAttributes(),
                S3StreamKafkaMetricsConstants.LABEL_TYPE_NAME);
        S3StreamKafkaMetricsManager.getInstance().registerConfigListener(usageBoundAttributes);
        S3StreamKafkaMetricsManager.getInstance().meter().gaugeBuilder(S3StreamKafkaMetricsManager.getInstance().prefix()
                        + S3StreamKafkaMetricsConstants.AUTO_BALANCER_METRICS_TIME_DELAY_METRIC_NAME)
                .setDescription("The time delay of auto balancer metrics per broker")
                .setUnit("ms")
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(S3StreamKafkaMetricsManager.getInstance().metricsConfig().getMetricsLevel())
                            && S3StreamKafkaMetricsManager.getInstance().isActiveController()) {
                        result.record(usageDistUpperBound, usageBoundAttributes.get(S3StreamKafkaMetricsConstants.UPPER_BOUND_TYPE_NAME));
                        result.record(usageDistLowerBound, usageBoundAttributes.get(S3StreamKafkaMetricsConstants.LOWER_BOUND_TYPE_NAME));
                    }
                });
    }

    @Override
    public void initialize(Set<BrokerUpdater.Broker> brokers) {
        Resource resource = resource();
        usageAvg = brokers.stream().mapToDouble(e -> e.load(resource)).sum() / brokers.size();
        usageDistLowerBound = Math.max(0, usageAvg * (1 - this.usageAvgDeviation));
        usageDistUpperBound = usageAvg * (1 + this.usageAvgDeviation);
    }

    @Override
    protected boolean requireLessLoad(BrokerUpdater.Broker broker) {
        return broker.load(resource()) > usageDistUpperBound;
    }

    @Override
    protected boolean requireMoreLoad(BrokerUpdater.Broker broker) {
        return broker.load(resource()) < usageDistLowerBound;
    }

    @Override
    public boolean isBrokerAcceptable(BrokerUpdater.Broker broker) {
        double load = broker.load(resource());
        if (load < this.usageDetectThreshold) {
            return true;
        }
        return load >= usageDistLowerBound && load <= usageDistUpperBound;
    }

    @Override
    public double brokerScore(BrokerUpdater.Broker broker) {
        double loadAvgDeviationAbs = Math.abs(usageAvg - broker.load(resource()));
        return GoalUtils.linearNormalization(loadAvgDeviationAbs, DEFAULT_MAX_LOAD_BYTES, 0, true);
    }

    @Override
    protected Comparator<BrokerUpdater.Broker> highLoadComparator() {
        return highLoadComparator;
    }

    @Override
    protected Comparator<BrokerUpdater.Broker> lowLoadComparator() {
        return lowLoadComparator;
    }
}
