from kafka import KafkaProducer
from faker import Faker
from enum import Enum
from sys import argv
import json

class GenMethod(Enum):
    UNIFORM = "uniform"
    CORRELATED = "correlated"
    ANTI_CORRELATED = "anti_correlated"

    @classmethod
    def from_str(cls, label):
        return cls(label.lower())

def generate_uniform_data(faker, dimensions, d_min, d_max):
    return [faker.random_int(min=d_min, max=d_max) for _ in range(dimensions)]

def generate_correlated_data(faker, dimensions, d_min, d_max):
    base = faker.random_int(min=d_min, max=d_max)
    offset = int((d_max - d_min) * 0.1)
    return [max(d_min, min(d_max, base + faker.random_int(min=-offset, max=offset))) for _ in range(dimensions)]

def generate_anti_correlated_data(faker, dimensions, d_min, d_max):
    rand_vals = [faker.random.random() for _ in range(dimensions)]
    target_sum = (d_min + d_max) / 2.0 * dimensions
    scale = target_sum / sum(rand_vals) if sum(rand_vals) != 0 else 1
    return [max(d_min, min(d_max, int(v * scale))) for v in rand_vals]

def generate_data():
    faker = Faker()
    topic_name = argv[1] if len(argv) > 1 else "input-tuples"
    method_str = argv[2] if len(argv) > 2 else "uniform"
    dimensions = int(argv[3]) if len(argv) > 3 else 2
    d_min = int(argv[4]) if len(argv) > 4 else 0
    d_max = int(argv[5]) if len(argv) > 5 else 1000

    generation_method = GenMethod.from_str(method_str)
    prod = KafkaProducer(bootstrap_servers="localhost:9092")

    print(f"Starting {generation_method.value} stream...")

    try:
        point_id = 0
        while True:
            if generation_method == GenMethod.UNIFORM:
                data = generate_uniform_data(faker, dimensions, d_min, d_max)
            elif generation_method == GenMethod.CORRELATED:
                data = generate_correlated_data(faker, dimensions, d_min, d_max)
            else:
                data = generate_anti_correlated_data(faker, dimensions, d_min, d_max)

            #update here
            payload = f"{point_id}," + ",".join(map(str, data))
            prod.send(topic_name, value=payload.encode('utf-8'))

            if point_id % 100000 == 0:
                print(f"Sent {point_id} records...")
            point_id += 1
    except KeyboardInterrupt:
        print("Stopping.")
    finally:
        prod.close()

if __name__ == '__main__':
    generate_data()
