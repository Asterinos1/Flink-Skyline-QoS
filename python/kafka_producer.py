from kafka import KafkaProducer
from faker import Faker
from enum import Enum
from sys import argv
import random
import time
import json

class GenMethod(Enum):
    UNIFORM = "uniform"
    CORRELATED = "correlated"
    ANTI_CORRELATED = "anti_correlated"

    @classmethod
    def from_str(cls, label):
        return cls(label.lower())

def generate_uniform_data(faker, dimensions, d_min=0, d_max=100):
    return [faker.random_int(min=d_min, max=d_max) for _ in range(dimensions)]

def generate_correlated_data(faker, dimensions, d_min=0, d_max=100):
    domain_range = d_max - d_min
    margin = int(domain_range * 0.1)

    safe_min = d_min + margin
    safe_max = d_max - margin
    
    
    if safe_min >= safe_max:
        safe_min, safe_max = d_min, d_max

    base = faker.random_int(min=safe_min, max=safe_max)

    offset_limit = int(domain_range * 0.1)
    # Minimum offset 1
    offset_limit = max(1, offset_limit)
    # Dimensions move together within a small range of the base
    return [base + faker.random_int(min=-offset_limit, max=offset_limit) for _ in range(dimensions)]

def generate_anti_correlated_data(faker, dimensions, d_min=0, d_max=100):
    rand_vals = [faker.random.random() for _ in range(dimensions)]
    target_sum = (d_min + d_max) / 2.0 * dimensions 
    current_sum = sum(rand_vals)
    scale = target_sum / current_sum if current_sum != 0 else 1
    point = []
    for v in rand_vals:
        val = v * scale
        noise = faker.random_int(min=int(-(d_max-d_min)*0.1), max=int((d_max-d_min)*0.1))
        val += noise
        val = max(d_min, min(d_max, int(val)))
        point.append(val)
        
    return point
def generate_data():
    faker = Faker()
    kafka_nodes = "localhost:9092"

    # Get arguments for generation method and dimensions
    topic_name = argv[1] if len(argv) > 1 else "skyline-data"
    method_str = argv[2] if len(argv) > 2 else "uniform"
    dimensions = int(argv[3]) if len(argv) > 3 else 2
    d_min = int(argv[4]) if len(argv) > 4 else 0
    d_max = int(argv[5]) if len(argv) > 5 else 100

    generation_method = GenMethod.from_str(method_str)

    # Use JSON serializer so Flink can parse it easily
    prod = KafkaProducer(
        bootstrap_servers=kafka_nodes,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # For High Throughput?(TEST)
        #batch_size=65536, 
        #linger_ms=20
    )
    print(f"Starting {generation_method.value} stream with {dimensions} dimensions...")
    


    try:
        point_id = 0
        while True:
            # Select the math based on Enum
            if generation_method == GenMethod.UNIFORM:
                data_points = generate_uniform_data(faker, dimensions, d_min, d_max)
            elif generation_method == GenMethod.CORRELATED:
                data_points = generate_correlated_data(faker, dimensions, d_min, d_max)
            elif generation_method == GenMethod.ANTI_CORRELATED:
                data_points = generate_anti_correlated_data(faker, dimensions, d_min, d_max)
            # Create the structured message
            '''
            message = {
                "id": point_id,
                "v": values,
                "ts": time.time()
            }
            '''

            #prod.send(topic_name, value=message)
            prod.send(topic_name, value=data_points)

            if point_id % 100000 == 0:
                print(f"Sent {point_id} records...")
            
            point_id += 1

    except KeyboardInterrupt:
        print("Stopping data generation.")
    finally:
        prod.flush()
        prod.close()
    

def main():
    generate_data()

if __name__ == '__main__':
    main()

