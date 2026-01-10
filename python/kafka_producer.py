from kafka import KafkaProducer
from faker import Faker
from enum import Enum
from sys import argv
import json

"""
Synthetic Data Stream Generator (Simple Version).

This script serves as a lightweight data producer for the Flink Skyline Experiment.
It continuously generates synthetic multi-dimensional data points using three distinct 
statistical distributions (Uniform, Correlated, Anti-Correlated) and pushes them 
to a specified Kafka topic. 

Unlike the more complex generator, this version focuses solely on data ingestion 
throughput and does not inject control signals or query triggers.
"""

class GenMethod(Enum):
    """
    Enumeration for supported data generation strategies.
    This ensures type safety when validating the distribution method provided via
    command-line arguments.
    """
    UNIFORM = "uniform"
    CORRELATED = "correlated"
    ANTI_CORRELATED = "anti_correlated"

    @classmethod
    def from_str(cls, label):
        """
        Safe utility to convert a string label into the corresponding Enum constant.
        It normalizes the input to lowercase to handle case-insensitive arguments.
        """
        return cls(label.lower())

"""
Generates a data point with independent random values for each dimension.

This function creates a vector where every dimension is selected uniformly at random
from the specified domain range. This represents a standard random distribution
where no specific relationship exists between the dimensions.
"""
def generate_uniform_data(faker, dimensions, d_min, d_max):
    return [faker.random_int(min=d_min, max=d_max) for _ in range(dimensions)]

"""
Generates a data point where dimensions are positively correlated.

The logic first selects a random 'base' value from the domain. It then generates
each dimension for the tuple by adding a small random offset to this base value.
The offset is constrained to ten percent of the total domain range.

The resulting points cluster tightly along the diagonal of the data space. This 
distribution typically results in a very small Skyline size because points with 
low base values tend to dominate the majority of the dataset.
"""
def generate_correlated_data(faker, dimensions, d_min, d_max):
    base = faker.random_int(min=d_min, max=d_max)
    # Define a small noise window (10% of domain) to maintain correlation
    offset = int((d_max - d_min) * 0.1)
    
    # Generate dimensions and clamp them to ensure they remain within [d_min, d_max]
    return [max(d_min, min(d_max, base + faker.random_int(min=-offset, max=offset))) for _ in range(dimensions)]

"""
Generates a data point where dimensions are negatively correlated.

This function attempts to place points on a hyperplane where the sum of all dimensions
remains roughly constant. It achieves this by generating a random vector and scaling it
so that its sum matches a target value (the average sum of the domain).

Points generated using this method cluster along the anti-diagonal. This represents the
most challenging scenario for Skyline computation because a point that is good in one
dimension is likely bad in others, resulting in very few dominance relationships.
"""
def generate_anti_correlated_data(faker, dimensions, d_min, d_max):
    rand_vals = [faker.random.random() for _ in range(dimensions)]
    
    # Calculate the target sum representing the center plane of the hypercube
    target_sum = (d_min + d_max) / 2.0 * dimensions
    
    # Determine the scaling factor required to project the random vector onto the plane
    current_sum = sum(rand_vals)
    scale = target_sum / current_sum if current_sum != 0 else 1
    
    # Apply scaling and clamp results to integer bounds
    return [max(d_min, min(d_max, int(v * scale))) for v in rand_vals]

"""
Main Execution Loop.

This function orchestrates the data generation process. It begins by parsing the 
command-line arguments to configure the Kafka topic, generation method, dimensionality, 
and domain boundaries.

Once configured, it initializes a Kafka Producer and enters an infinite loop. 
In each iteration, it generates a single tuple using the selected statistical method,
serializes it into a CSV format string (ID, Val1, Val2...), and emits it to the 
Kafka broker. Progress is logged to the console every one hundred thousand records.
"""
def generate_data():
    faker = Faker()
    
    # CLI Argument Parsing with fallback defaults
    topic_name = argv[1] if len(argv) > 1 else "input-tuples"
    method_str = argv[2] if len(argv) > 2 else "uniform"
    dimensions = int(argv[3]) if len(argv) > 3 else 2
    d_min = int(argv[4]) if len(argv) > 4 else 0
    d_max = int(argv[5]) if len(argv) > 5 else 1000

    generation_method = GenMethod.from_str(method_str)
    
    # Initialize connection to local Kafka Broker
    prod = KafkaProducer(bootstrap_servers="localhost:9092")

    print(f"Starting {generation_method.value} stream...")

    try:
        point_id = 0
        while True:
            # Delegate generation to the specific statistical helper function
            if generation_method == GenMethod.UNIFORM:
                data = generate_uniform_data(faker, dimensions, d_min, d_max)
            elif generation_method == GenMethod.CORRELATED:
                data = generate_correlated_data(faker, dimensions, d_min, d_max)
            else:
                data = generate_anti_correlated_data(faker, dimensions, d_min, d_max)

            # Construct CSV Payload: "ID,Dim1,Dim2,Dim3..."
            payload = f"{point_id}," + ",".join(map(str, data))
            
            # Send serialized bytes to Kafka
            prod.send(topic_name, value=payload.encode('utf-8'))

            # Periodic progress logging
            if point_id % 100000 == 0:
                print(f"Sent {point_id} records...")
            point_id += 1
            
    except KeyboardInterrupt:
        print("Stopping.")
    finally:
        # Resource Cleanup: Close the producer connection cleanly
        prod.close()

if __name__ == '__main__':
    generate_data()
