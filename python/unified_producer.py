import random
from kafka import KafkaProducer
from faker import Faker
from enum import Enum
from sys import argv
import time

"""
Synthetic Data Stream Generator for Skyline Queries.

This script acts as the primary data producer for the Flink Skyline Experiment.
It generates synthetic multi-dimensional data points using three distinct statistical distributions:
- Uniform (Randomly distributed)
- Correlated (Points cluster along the diagonal, easier to prune)
- Anti-Correlated (Points cluster along the anti-diagonal, hardest to prune/skyline)

Additionally, it automatically injects "Query Triggers" into a separate control topic
at defined intervals (QUERY_THRESHOLD), allowing the Flink job to perform latency
and throughput measurements at regular checkpoints.
"""

# Configuration Constant
# Defines how many data records to send before triggering a performance snapshot (Query).
# Example: If set to 1,000,000, a query is fired after every 1 million records.
QUERY_THRESHOLD = 1000000 

"""
    Enumeration for supported data generation strategies.
    Ensures type safety when selecting the distribution method from CLI arguments.
"""
class GenMethod(Enum):
    UNIFORM = "uniform"
    CORRELATED = "correlated"
    ANTI_CORRELATED = "anti_correlated"

    """
        Safe converter from string input (CLI arg) to Enum constant.
        Default behavior (if not matched) should be handled by the caller or fail fast.
    """
    @classmethod
    def from_str(cls, label):
        return cls(label.lower())

"""
Generates a data point with independent random values for each dimension.

Points are spread evenly across the domain [d_min, d_max]. Dominance checks against
this distribution typically exhibit average-case complexity.
"""
def generate_uniform_data(faker, dimensions, d_min, d_max):
    return [faker.random_int(min=d_min, max=d_max) for _ in range(dimensions)]

"""
Generates a data point where dimensions are positively correlated.

The logic picks a base value and generates all other dimensions by adding small random noise
to that base. The noise range is determined by the correlation factor (rho), where a higher
rho results in tighter clustering along the diagonal.

These points lie close to the diagonal (e.g., x ~= y ~= z). They are computationally 
easy to prune because "good" points (low x, low y) dominate almost everything else.
"""
def generate_correlated_data(faker, dimensions, d_min, d_max, rho=0.9):
    base = random.uniform(d_min, d_max)

    data = []
    for _ in range(dimensions):
        noise = random.uniform(
            -(1 - rho) * (d_max - d_min),
            +(1 - rho) * (d_max - d_min)
        )
        val = base + noise
        # Clamp value to ensure it stays within domain bounds
        data.append(max(d_min, min(d_max, int(val))))

    return data


"""
Generates a data point where dimensions are negatively correlated.

This function approximates a plane (or hyperplane in >3D) where the sum of all dimensions
is roughly constant. This creates a scenario where if one dimension is small (good), 
the others must be large (bad).

Points generated here cluster along the anti-diagonal. This represents the "worst-case" 
scenario for Skyline algorithms because most points are non-dominated. 
The 'epsilon' parameter controls the thickness of the anti-correlated band; higher dimensions 
require a thicker slice to ensure valid integer points can be found.
"""
def generate_anti_correlated_data(faker, dimensions, d_min, d_max):
    # Heuristic tuning for 'thickness' of the anti-correlated band.
    epsilon = 0.0005 

    if dimensions == 2:
        epsilon = 0.0005
    elif dimensions == 3:
        epsilon = 0.05    # 3D needs more slack
    elif dimensions == 4:
        epsilon = 0.9     # 4D needs significantly more slack
    else:
        epsilon = dimensions * 0.005 * 100 # Scaling heuristic for high dims

    # Generate a random direction vector
    vals = [faker.random.random() for _ in range(dimensions)]
    total = sum(vals)

    # Define a target sum (The Anti-Diagonal Plane)
    # The 'mean' represents the center of the hypercube.
    # We add 'slack' (epsilon) to give the plane some volume/thickness.
    mean = (d_min + d_max) / 2.0 * dimensions
    slack = epsilon * (d_max - d_min) * dimensions
    target_sum = random.uniform(mean - slack, mean + slack)

    # Scale the random vector to meet the target sum
    scale = target_sum / total if total != 0 else 1.0
    scaled = [v * scale for v in vals]

    # Clamp values to domain and return integers
    return [
        max(d_min, min(d_max, int(v)))
        for v in scaled
    ]

"""
Main Execution Loop.

The function parses CLI arguments, initializes the Kafka Producer, and enters an infinite 
loop to generate and stream data. It also monitors the number of records sent and 
automatically injects a 'Query Trigger' message into the control topic whenever the 
count reaches the configured QUERY_THRESHOLD.
"""
def run_generator():
    faker = Faker()
    
    # CLI Argument Parsing with defaults
    data_topic = argv[1] if len(argv) > 1 else "input-tuples"
    method_str = argv[2] if len(argv) > 2 else "uniform"
    dimensions = int(argv[3]) if len(argv) > 3 else 2
    d_min = int(argv[4]) if len(argv) > 4 else 0
    d_max = int(argv[5]) if len(argv) > 5 else 1000
    query_topic = argv[6] if len(argv) > 6 else "queries"
    
    generation_method = GenMethod.from_str(method_str)
    
    # Initialize connection to Kafka Broker
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
            # Generate Data Vector based on selected method
            if generation_method == GenMethod.UNIFORM:
                data = generate_uniform_data(faker, dimensions, d_min, d_max)
            elif generation_method == GenMethod.CORRELATED:
                data = generate_correlated_data(faker, dimensions, d_min, d_max)
            else:
                data = generate_anti_correlated_data(faker, dimensions, d_min, d_max)

            # Serialize and Send Data Tuple
            # Format: "ID,Val1,Val2,Val3..." (e.g., "1001,500,200")
            payload = f"{point_id}," + ",".join(map(str, data))
            prod.send(data_topic, value=payload.encode('utf-8'))
            
            point_id += 1

            # Check for Query Trigger Condition
            if point_id % QUERY_THRESHOLD == 0:
                # Construct Query Payload
                # Format: "QueryID,RequiredRecordCount" (e.g., "1,1000000")
                # This tells the Flink Job: "Run Query #1, but only after you have processed record #1000000"
                query_payload = f"{query_id},{point_id}"
                prod.send(query_topic, value=query_payload.encode('utf-8'))
                
                print(f"[Trigger] Sent {point_id} records. Fired Query ID: {query_id}")
                query_id += 1
            
            # Progress Logging (Every 100k records)
            if point_id % 100000 == 0 and point_id % QUERY_THRESHOLD != 0:
                 print(f"Sent {point_id} records...")

    except KeyboardInterrupt:
        print("\nStopping stream.")
    finally:
        # Resource Cleanup: Ensure all messages are flushed to network before exit
        prod.flush()
        prod.close()

if __name__ == '__main__':
    run_generator()
