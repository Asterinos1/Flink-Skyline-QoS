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
import java.util.Arrays;
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

        // Empirically partitions set to 2x number of nodes (parallelism)
        final int numPartitions = parallelism * 2;

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

        // 2. Partitioning Logic
        DataStream<ServiceTuple> processedData = rawData;
        PartitioningLogic.SkylinePartitioner partitioner;

        switch (algo) {
            case "mr-dim":
                // MR-Dim: Standard dimensional partitioning
                partitioner = new PartitioningLogic.DimPartitioner(numPartitions, domainMax);
                break;
            case "mr-grid":
                // MR-Grid: Prune dominated grids FIRST
                processedData = rawData.filter(new PartitioningLogic.GridDominanceFilter(domainMax, dims));
                partitioner = new PartitioningLogic.GridPartitioner(numPartitions, domainMax, dims);
                break;
            default:
                // MR-Angle: Hyperspherical partitioning
                partitioner = new PartitioningLogic.AnglePartitioner(numPartitions, dims);
                break;
        }

        KeyedStream<ServiceTuple, Integer> keyedData = processedData.keyBy(partitioner);

        // 3. Query Trigger Stream
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

        // 4. Local Skyline Computation
        DataStream<Tuple3<String, Long, List<ServiceTuple>>> localSkylines = keyedData
                .connect(keyedTriggers)
                .process(new SkylineLocalProcessor())
                .name("LocalSkylineProcessor");

        // 5. Global Aggregation
        DataStream<String> finalResults = localSkylines
                .keyBy(t -> t.f0) // Key by Query ID
                .process(new GlobalSkylineAggregator(numPartitions))
                .name("GlobalReducer");

        // 6. Sink
        finalResults.sinkTo(KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                // Added configuration for 10 MB (10 * 1024 * 1024 bytes)
                .setProperty("max.request.size", "10485760")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema()).build())
                .build());

        env.execute("Flink Skyline: " + algo);
    }

    // ------------------------------------------------------------------------
    // LOCAL PROCESSOR (BNL Algorithm)
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
            if (!inputBuffer.isEmpty()) {
                processBuffer();
            }

            String queryId = trigger.f1;
            Long startTime = trigger.f2;
            List<ServiceTuple> results = new ArrayList<>();

            for (ServiceTuple s : localSkylineState.get()) {
                results.add(s);
            }

            out.collect(new Tuple3<>(queryId, startTime, results));
        }

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
    // GLOBAL AGGREGATOR (Merges local skylines)
    // ------------------------------------------------------------------------
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

            List<ServiceTuple> incoming = input.f2;
            long startTime = input.f1;

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

            if (count + 1 >= totalPartitions) {
                long endTime = System.currentTimeMillis();
                long duration = endTime - startTime;

                StringBuilder sb = new StringBuilder();
                sb.append("{\"query_id\": \"").append(ctx.getCurrentKey()).append("\", ");
                sb.append("\"latency_ms\": ").append(duration).append(", ");
                sb.append("\"skyline_size\": ").append(currentGlobal.size()).append(", ");
//                sb.append("\n\"skyline\": [");
//
//                for(int i=0; i<currentGlobal.size(); i++) {
//                    ServiceTuple s = currentGlobal.get(i);
//                    sb.append("{\"id\":\"").append(s.id).append("\", \"val\":").append(Arrays.toString(s.values)).append("}");
//                    if(i < currentGlobal.size() - 1) sb.append(",");
//                }
//                sb.append("]}");

                out.collect(sb.toString());

                globalBuffer.clear();
                arrivedCount.clear();
            }
        }
    }

    // ------------------------------------------------------------------------
    // PARTITIONING LOGIC (Corrected MR-Angle Logic)
    // ------------------------------------------------------------------------
    public static class PartitioningLogic implements Serializable {
        public interface SkylinePartitioner extends KeySelector<ServiceTuple, Integer> { }

        // --- MR-Dim ---
        public static class DimPartitioner implements SkylinePartitioner {
            private final int partitions;
            private final double maxVal;

            public DimPartitioner(int partitions, double maxVal) {
                this.partitions = partitions;
                this.maxVal = maxVal;
            }

            @Override
            public Integer getKey(ServiceTuple t) {
                int p = (int) (t.values[0] / (maxVal / partitions));
                return Math.max(0, Math.min(p, partitions - 1));
            }
        }

        // --- MR-Grid ---
        public static class GridDominanceFilter extends RichFilterFunction<ServiceTuple> {
            private final double threshold;
            public GridDominanceFilter(double maxVal, int dims) {
                this.threshold = maxVal / 2.0;
            }
            @Override
            public boolean filter(ServiceTuple t) {
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

        // --- MR-Angle: Hyperspherical Partitioning (FIXED) ---
        public static class AnglePartitioner implements SkylinePartitioner {
            private final int partitions;
            private final int dims;

            public AnglePartitioner(int partitions, int dims) {
                this.partitions = partitions;
                this.dims = dims;
            }

            @Override
            public Integer getKey(ServiceTuple t) {
                // For N dimensions, there are N-1 angles
                int numAngles = dims - 1;

                // If 1D data (unlikely for skyline), return 0
                if (numAngles < 1) return 0;

                // 1. Calculate all angles phi_1 to phi_{n-1} based on Equation (1)
                // Note: Paper uses 1-based index. Java is 0-based.
                // phi_i corresponds to the angle between axis i and the rest of the vector.
                double[] angles = new double[numAngles];

                for (int i = 0; i < numAngles; i++) {
                    double v_i = t.values[i];

                    // Calculate magnitude of the remaining dimensions (v_{i+1} ... v_n)
                    double sumSqRest = 0.0;
                    for (int j = i + 1; j < dims; j++) {
                        sumSqRest += t.values[j] * t.values[j];
                    }
                    double hyp = Math.sqrt(sumSqRest);

                    // Calculate angle using atan2 (returns -pi to pi, but data is positive so 0 to pi/2)
                    angles[i] = Math.atan2(hyp, v_i);
                }

                // 2. Map the Angular Vector to a Partition ID
                // "Modify the grid partitioning over the n-1 subspaces"
                // We treat the angular coordinates as a new grid space and linearize it.

                // Determine splits per angular dimension to fit roughly into total partitions
                // If we have many dims and few partitions, we prioritize the first angles.
                // Simple linearization strategy:
                // Normalize angle to [0, 1) (dividing by PI/2) -> scale by splits -> compute index

                // Heuristic: Distribute cuts across dimensions.
                // For robustness with variable inputs, we use a mixed-radix-like hashing
                // or simply sum the weighted sectors to ensure all angles contribute.

                double maxAngle = Math.PI / 2.0;
                long linearizedID = 0;

                // We split the angular space into a grid.
                // We use a base-2 grid (high/low) for each angle if partitions allow,
                // or simply map the continuous angular values to the integer range.

                // Robust Implementation:
                // We map the multi-dimensional angular coordinate to a single linear value
                // by conceptually dividing the angular hypersphere into sectors.

                // Step A: Normalize all angles to 0.0 -> 1.0 range
                double normalizedSum = 0.0;
                for(int k=0; k < numAngles; k++) {
                    // weighted position: earlier angles often separate space more significantly in HS coords
                    normalizedSum += (angles[k] / maxAngle);
                }

                // Step B: Average normalized position to find "sector" in the linear sequence
                double avgPosition = normalizedSum / numAngles;

                // Step C: Map to partition range
                int p = (int) (avgPosition * partitions);

                // Alternative Rigorous Grid Implementation (if N is large enough):
                // If strictly following "Grid on Subspaces", we would do:
                // int id = 0;
                // for(double ang : angles) { if(ang > PI/4) id = (id << 1) | 1; else id = id << 1; }
                // return id % partitions;

                // Returning the mapping based on the aggregated angular position
                return Math.max(0, Math.min(p, partitions - 1));
            }
        }
    }
}
