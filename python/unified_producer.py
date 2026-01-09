import random
from kafka import KafkaProducer
from faker import Faker
from enum import Enum
from sys import argv
import time

QUERY_THRESHOLD = 1000000  # Trigger a query every QUERY_THRESHOLD tuples sent

class GenMethod(Enum):
    UNIFORM = "uniform"
    CORRELATED = "correlated"
    ANTI_CORRELATED = "anti_correlated"

    @classmethod
    def from_str(cls, label):
        return cls(label.lower())

def generate_uniform_data(faker, dimensions, d_min, d_max):
    return [faker.random_int(min=d_min, max=d_max) for _ in range(dimensions)]

def generate_correlated_data(faker, dimensions, d_min, d_max, rho=0.9):
    base = random.uniform(d_min, d_max)

    data = []
    for _ in range(dimensions):
        noise = random.uniform(
            -(1 - rho) * (d_max - d_min),
            +(1 - rho) * (d_max - d_min)
        )
        val = base + noise
        data.append(max(d_min, min(d_max, int(val))))

    return data


def generate_anti_correlated_data(faker, dimensions, d_min, d_max):
    epsilon = max(0.006, 0.003 * dimensions)

    # Step 1: Random base vector
    vals = [faker.random.random() for _ in range(dimensions)]
    total = sum(vals)

    # Step 2: Target sum with slack
    mean = (d_min + d_max) / 2.0 * dimensions
    slack = epsilon * (d_max - d_min) * dimensions
    target_sum = random.uniform(mean - slack, mean + slack)

    # Step 3: Scale
    scale = target_sum / total if total != 0 else 1.0
    scaled = [v * scale for v in vals]

    # Step 4: Clamp
    return [
        max(d_min, min(d_max, int(v)))
        for v in scaled
    ]

def run_generator():
    faker = Faker()
    data_topic = argv[1] if len(argv) > 1 else "input-tuples"
    method_str = argv[2] if len(argv) > 2 else "uniform"
    dimensions = int(argv[3]) if len(argv) > 3 else 2
    d_min = int(argv[4]) if len(argv) > 4 else 0
    d_max = int(argv[5]) if len(argv) > 5 else 1000
    query_topic = argv[6] if len(argv) > 6 else "queries"
    generation_method = GenMethod.from_str(method_str)
    prod = KafkaProducer(bootstrap_servers="localhost:9092")

    print(f"--- Configuration ---")
    print(f"Data Topic:  {data_topic}")
    print(f"Query Topic: {query_topic}")
    print(f"Method:      {generation_method.value}")
    print(f"Dimensions:  {dimensions}")
    print(f"Domain:      [{d_min}, {d_max}]")
    print(f"Threshold:   Query every {QUERY_THRESHOLD} records")
    print(f"---------------------")
    print("Starting stream...")

    try:
        point_id = 0
        query_id = 1 
        
        while True:
            if generation_method == GenMethod.UNIFORM:
                data = generate_uniform_data(faker, dimensions, d_min, d_max)
            elif generation_method == GenMethod.CORRELATED:
                data = generate_correlated_data(faker, dimensions, d_min, d_max)
            else:
                data = generate_anti_correlated_data(faker, dimensions, d_min, d_max)

            payload = f"{point_id}," + ",".join(map(str, data))
            prod.send(data_topic, value=payload.encode('utf-8'))
            
            point_id += 1

            if point_id % QUERY_THRESHOLD == 0:
                # Format: "query_id,current_point_count"
                query_payload = f"{query_id},{point_id}"
                prod.send(query_topic, value=query_payload.encode('utf-8'))
                
                print(f"[Trigger] Sent {point_id} records. Fired Query ID: {query_id}")
                query_id += 1
            
            if point_id % 100000 == 0 and point_id % QUERY_THRESHOLD != 0:
                 print(f"Sent {point_id} records...")

    except KeyboardInterrupt:
        print("\nStopping stream.")
    finally:
        prod.flush()
        prod.close()

if __name__ == '__main__':
    run_generator()
