package org.main;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.util.*;

public class FlinkSkyline {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //fixed parameters
        int parallelism = 4;
        String algo = "angle"; //algorihtm options: "dim", "grid", "angle"
        env.setParallelism(parallelism);

        //kafka topics for inserting tuples and query triggers.
        KafkaSource<String> tupleSrc = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("input-tuples")
                .setStartingOffsets(OffsetsInitializer.earliest()) // ADD THIS LINE
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> querySrc = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092").setTopics("queries")
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();

        //setting tuples stream
        //it converts the input data (String) to tuples using the ServiceTuple class we created
        DataStream<ServiceTuple> tuples = env.fromSource(tupleSrc,
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Tuples")
                .flatMap((String s, Collector<ServiceTuple> out) -> {
                    try {
                        String[] p = s.split(",");
                        out.collect(new ServiceTuple(p[0], new double[]{Double.parseDouble(p[1]), Double.parseDouble(p[2])}));
                    } catch (Exception e) {}
                }).returns(ServiceTuple.class);

        //broadcast queries, basically message all workers that it's time to start processing the stream using the algorithm
        //(using broadcast means we message all processes)
        MapStateDescriptor<String, String> desc = new MapStateDescriptor<>("q", String.class, String.class);
        BroadcastStream<String> bQueries = env.fromSource(querySrc, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Queries").broadcast(desc);

        //partion data depending on the algorithm choice
        DataStream<ServiceTuple> partitioned = tuples.keyBy(t -> {
            switch (algo) {
                case "dim": //MR-Dim algorithm
                    return (int) (t.values[0] / (1000.0 / parallelism));
                case "grid": //MR-Grid algorithm
                    return (int) (t.values[0] / 500) + (int) (t.values[1] / 500) * 2;
                case "angle": //MR-Angle algorithm
                default:
                    //we set a default case to be the MR-Angle
                    //compute the partition Pi that sn belongs to based on
                    //the service sn’s coordinate value
                    double angle = Math.atan2(t.values[1], t.values[0]);
                    return (int) (angle / (Math.PI / 2 / parallelism));
            }
        });

        //connect and process
        partitioned.connect(bQueries)
                .process(new SkylineProcessor()) //use the custom SkylineProcessor class below (.process is a HOF)
                .sinkTo(KafkaSink.<String>builder()
                        .setBootstrapServers("localhost:9092")
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic("output-skyline").setValueSerializationSchema(new SimpleStringSchema()).build())
                        .build());
        env.execute("Distributed Skyline - " + algo);
    }

    //Local Skyline Computation
    //we create the custom class SkylineProcessor based on the interface KeyedBroadcastProcessFunction
    //(this is expected when using .process() above.)
    //we use this implementation in order create a stateful operator since we are handling 2 input streams (tuples and queries)
    //(note to sunadelfos: using lambda functions could be messier or ronaldo-ier)

    public static class SkylineProcessor extends KeyedBroadcastProcessFunction<Integer, ServiceTuple, String, String> {
        private transient ListState<ServiceTuple> skyState;
        //creating functions according to the interface.
        @Override
        public void open(Configuration parameters) {
            skyState = getRuntimeContext().getListState(new ListStateDescriptor<>("sky", ServiceTuple.class));
        }

        //BNL part here
        @Override
        public void processElement(ServiceTuple value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
            List<ServiceTuple> current = new ArrayList<>();
            boolean isDominated = false;
            //BNL is done here exactly, compare each tuple with every other
            for (ServiceTuple s : skyState.get()) {
                if (s.dominates(value))
                {
                    isDominated = true;
                    break;
                }
                if (!value.dominates(s)){
                    current.add(s);
                }
            }
            if (!isDominated) {
                current.add(value);
                skyState.update(current);
            }
        }

        @Override
        public void processBroadcastElement(String query, Context ctx, Collector<String> out) throws Exception {
            //safely accesses the keyed state from a broadcast context
            //we print the local skyline for each partition
            ctx.applyToKeyedState(new ListStateDescriptor<>("sky", ServiceTuple.class),
                    (Integer key, ListState<ServiceTuple> state) -> {
                        for (ServiceTuple s : state.get()) {
                            out.collect("QID " + query + " [" + key + "]: " + s.toString());
                        }
                    });
        }
    }
}