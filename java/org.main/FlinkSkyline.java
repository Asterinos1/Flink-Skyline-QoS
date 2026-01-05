package org.main;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class FlinkSkyline {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // --- Parameters ---
        final int parallelism = params.getInt("parallelism", 4);
        final String algo = params.get("algo", "mr-angle").toLowerCase();
        final String inputTopic = params.get("input-topic", "input-tuples");
        final String queryTopic = params.get("query-topic", "queries");
        final String outputTopic = params.get("output-topic", "output-skyline");
        final double domainMax = params.getDouble("domain", 1000.0);
        final int dims = params.getInt("dims", 2);

        final int numPartitions = parallelism * 2; //this is what it says in the paper

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // --- Kafka Sources ---
        KafkaSource<String> tupleSrc = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(inputTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> querySrc = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(queryTopic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 1. Ingest and Parse
        DataStream<ServiceTuple> rawData = env.fromSource(tupleSrc, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Data")
                .map(ServiceTuple::fromString)
                .filter(Objects::nonNull);

        // 2. Partitioning Logic (From Version 1 - Correct Paper Implementation)
        DataStream<ServiceTuple> processedData = rawData;
        PartitioningLogic.SkylinePartitioner partitioner;

        switch (algo) {
            case "mr-dim":
                // MR-Dim: Standard dimensional partitioning
                partitioner = new PartitioningLogic.DimPartitioner(numPartitions, domainMax);
                break;
            case "mr-grid":
                // MR-Grid: Prune dominated grids FIRST [cite: 231]
                processedData = rawData.filter(new PartitioningLogic.GridDominanceFilter(domainMax, dims));
                partitioner = new PartitioningLogic.GridPartitioner(numPartitions, domainMax, dims);
                break;
            default:
                // MR-Angle: Hyperspherical partitioning [cite: 191]
                partitioner = new PartitioningLogic.AnglePartitioner(numPartitions, dims);
                break;
        }

        KeyedStream<ServiceTuple, Integer> keyedData = processedData.keyBy(partitioner);

        // 3. Query Trigger Stream
        // Using Tuple3 to track Start Time for Latency Metrics
        KeyedStream<Tuple3<Integer, String, Long>, Integer> keyedTriggers = env
                .fromSource(querySrc, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Queries")
                .flatMap(new FlatMapFunction<String, Tuple3<Integer, String, Long>>() {
                    @Override
                    public void flatMap(String queryId, Collector<Tuple3<Integer, String, Long>> out) {
                        long startTime = System.currentTimeMillis();
                        // Broadcast query to all partitions
                        for (int i = 0; i < numPartitions; i++) {
                            out.collect(new Tuple3<>(i, queryId, startTime));
                        }
                    }
                })
                .keyBy(t -> t.f0);

        // 4. Local Skyline Computation (From Version 1 - Keeps Time info)
        DataStream<Tuple3<String, Long, List<ServiceTuple>>> localSkylines = keyedData
                .connect(keyedTriggers)
                .process(new SkylineLocalProcessor())
                .name("LocalSkylineProcessor");

        // 5. Global Aggregation (From Version 2 - Barrier Synchronization)
        DataStream<String> finalResults = localSkylines
                .keyBy(t -> t.f0) // Key by Query ID
                .process(new GlobalSkylineAggregator(numPartitions)) // Pass numPartitions for barrier. wait numPartitions answers
                .name("GlobalReducer");

        // 6. Sink
        finalResults.sinkTo(KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema()).build())
                .build());

        env.execute("Flink Skyline: " + algo);
    }

    // ------------------------------------------------------------------------
    // LOCAL PROCESSOR (Version 1 Logic)
    // ------------------------------------------------------------------------
    public static class SkylineLocalProcessor extends CoProcessFunction<ServiceTuple, Tuple3<Integer, String, Long>, Tuple3<String, Long, List<ServiceTuple>>> {

        private transient ListState<ServiceTuple> localSkylineState;
        private transient List<ServiceTuple> inputBuffer;
        private final int BUFFER_SIZE = 5000;

        @Override
        public void open(Configuration config) {
            localSkylineState = getRuntimeContext().getListState(new ListStateDescriptor<>("localSky", ServiceTuple.class));
            inputBuffer = new ArrayList<>();
        }

        @Override
        public void processElement1(ServiceTuple point, Context ctx, Collector<Tuple3<String, Long, List<ServiceTuple>>> out) throws Exception {
            inputBuffer.add(point);
            if (inputBuffer.size() >= BUFFER_SIZE) {
                processBuffer();
            }
        }

        @Override
        public void processElement2(Tuple3<Integer, String, Long> trigger, Context ctx, Collector<Tuple3<String, Long, List<ServiceTuple>>> out) throws Exception {
            // Force flush buffer before calculating skyline
            if (!inputBuffer.isEmpty()) {
                processBuffer();
            }

            String queryId = trigger.f1;
            Long startTime = trigger.f2; // Preserve start time
            List<ServiceTuple> results = new ArrayList<>();

            for (ServiceTuple s : localSkylineState.get()) {
                results.add(s);
            }

            // Send local results + original start time to Global Reducer
            out.collect(new Tuple3<>(queryId, startTime, results));
        }


        //BNL Algo?
        private void processBuffer() throws Exception {
            Iterable<ServiceTuple> stateIter = localSkylineState.get();
            List<ServiceTuple> currentSkyline = new ArrayList<>();
            if (stateIter != null) {
                for (ServiceTuple s : stateIter) currentSkyline.add(s);
            }

            for (ServiceTuple candidate : inputBuffer) {
                boolean isDominated = false;
                Iterator<ServiceTuple> it = currentSkyline.iterator();
                while (it.hasNext()) {
                    ServiceTuple existing = it.next();
                    if (existing.dominates(candidate)) {
                        isDominated = true;
                        break;
                    }
                    if (candidate.dominates(existing)) {
                        it.remove();
                    }
                }
                if (!isDominated) {
                    currentSkyline.add(candidate);
                }
            }

            localSkylineState.update(currentSkyline);
            inputBuffer.clear();
        }
    }

    // ------------------------------------------------------------------------
    // GLOBAL AGGREGATOR (Version 2 Logic - Barrier Sync)
    // ------------------------------------------------------------------------
    // Input: Tuple3<QueryID, StartTime, LocalSkylineList>
    public static class GlobalSkylineAggregator extends KeyedProcessFunction<String, Tuple3<String, Long, List<ServiceTuple>>, String> {

        private final int totalPartitions;
        private transient ValueState<List<ServiceTuple>> globalBuffer;
        private transient ValueState<Integer> arrivedCount;

        public GlobalSkylineAggregator(int totalPartitions) {
            this.totalPartitions = totalPartitions;
        }

        @Override
        public void open(Configuration config) {
            globalBuffer = getRuntimeContext().getState(new ValueStateDescriptor<>("gBuffer", TypeInformation.of(new TypeHint<List<ServiceTuple>>() {})));
            arrivedCount = getRuntimeContext().getState(new ValueStateDescriptor<>("cnt", Integer.class));
        }

        @Override
        public void processElement(Tuple3<String, Long, List<ServiceTuple>> input, Context ctx, Collector<String> out) throws Exception {
            List<ServiceTuple> currentGlobal = globalBuffer.value();
            if (currentGlobal == null) currentGlobal = new ArrayList<>();

            Integer count = arrivedCount.value();
            if (count == null) count = 0;

            List<ServiceTuple> incoming = input.f2; // Local skyline list
            long startTime = input.f1;              // Start time from Source

            // 1. Merge incoming local skyline into global buffer
            if (incoming != null && !incoming.isEmpty()) {
                for (ServiceTuple candidate : incoming) {
                    boolean isDominated = false;
                    Iterator<ServiceTuple> it = currentGlobal.iterator();
                    while (it.hasNext()) {
                        ServiceTuple existing = it.next();
                        if (existing.dominates(candidate)) {
                            isDominated = true;
                            break;
                        }
                        if (candidate.dominates(existing)) {
                            it.remove();
                        }
                    }
                    if (!isDominated) {
                        currentGlobal.add(candidate);
                    }
                }
            }

            globalBuffer.update(currentGlobal);
            arrivedCount.update(count + 1);

            // 2. Barrier Check: Only emit when ALL partitions have reported
            if (count + 1 >= totalPartitions) {
                long endTime = System.currentTimeMillis();
                long duration = endTime - startTime;

                StringBuilder sb = new StringBuilder();
                sb.append("{\"query_id\": \"").append(ctx.getCurrentKey()).append("\", ");
                sb.append("\"latency_ms\": ").append(duration).append(", ");
                sb.append("\"skyline_size\": ").append(currentGlobal.size()).append(", ");
                sb.append("\n\"skyline\": [");

                for(int i=0; i<currentGlobal.size(); i++) {
                    ServiceTuple s = currentGlobal.get(i);
                    sb.append("{\"id\":\"").append(s.id).append("\", \"val\":").append(java.util.Arrays.toString(s.values)).append("}");
                    if(i < currentGlobal.size() - 1) sb.append(",");
                }
                sb.append("]}");

                out.collect(sb.toString());

                // Clear state for this query ID to free memory
                globalBuffer.clear();
                arrivedCount.clear();
            }
        }
    }

    // ------------------------------------------------------------------------
    // PARTITIONING LOGIC (Version 1 Logic - Paper Correct)
    // ------------------------------------------------------------------------
    public static class PartitioningLogic implements Serializable {
        public interface SkylinePartitioner extends KeySelector<ServiceTuple, Integer> { }

        // --- MR-Dim: 1D Partitioning ---
        public static class DimPartitioner implements SkylinePartitioner {
            private final int partitions;
            private final double maxVal;

            public DimPartitioner(int partitions, double maxVal) {
                this.partitions = partitions;
                this.maxVal = maxVal;
            }

            @Override
            public Integer getKey(ServiceTuple t) {
                // [cite: 220] Range = Vmax/N
                int p = (int) (t.values[0] / (maxVal / partitions));
                return Math.max(0, Math.min(p, partitions - 1));
            }
        }

        // --- MR-Grid: Pruning Filter + Partitioner ---
        // [cite: 231] "we do not have to compute the local skyline of the up-right partition"
        public static class GridDominanceFilter extends RichFilterFunction<ServiceTuple> {
            private final double threshold;
            public GridDominanceFilter(double maxVal, int dims) {
                this.threshold = maxVal / 2.0;
            }
            @Override
            public boolean filter(ServiceTuple t) {
                // If all values are > threshold (upper right), prune it.
                boolean allWorse = true;
                for(double v : t.values) {
                    if (v < threshold) {
                        allWorse = false;
                        break;
                    }
                }
                return !allWorse;
            }
        }

        public static class GridPartitioner implements SkylinePartitioner {
            private final int partitions;
            private final double threshold;

            public GridPartitioner(int partitions, double maxVal, int dims) {
                this.partitions = partitions;
                this.threshold = maxVal / 2.0;
            }

            @Override
            public Integer getKey(ServiceTuple t) {
                int gridID = 0;
                for (int i = 0; i < t.values.length; i++) {
                    if (t.values[i] >= threshold) {
                        gridID |= (1 << i);
                    }
                }
                return Math.abs(gridID) % partitions;
            }
        }

        // --- MR-Angle: Hyperspherical Partitioning ---
        public static class AnglePartitioner implements SkylinePartitioner {
            private final int partitions;
            private final int dims;

            public AnglePartitioner(int partitions, int dims) {
                this.partitions = partitions;
                this.dims = dims;
            }

            @Override
            public Integer getKey(ServiceTuple t) {
                // [cite: 191] Equation 1 implementation
                double r = 0.0;
                for(double v : t.values) r += v*v;
                r = Math.sqrt(r);

                if (r == 0) return 0;

                // tan(phi_1) = sqrt(sum_sq_rest) / v_1
                double sumSqRest = 0;
                for (int i = 1; i < t.values.length; i++) {
                    sumSqRest += t.values[i] * t.values[i];
                }

                // Use atan2 for correct quadrant handling
                double phi_1 = Math.atan2(Math.sqrt(sumSqRest), t.values[0]);

                double maxAngle = Math.PI / 2.0;
                int p = (int) (phi_1 / (maxAngle / partitions));

                return Math.max(0, Math.min(p, partitions - 1));
            }
        }
    }
}
