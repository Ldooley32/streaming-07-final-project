"""
    This program listens for work messages continuously.
    The message is modified and logged in a different csv file. 
    Fictional cost where energy is $1 for every 1500 Mega Watts (MW) 
    used is detemined from the MW in the data used.   

    Author: Laura Dooley
    Date: June 12, 2024
    
"""

import pika
import sys
import csv
from pathlib import Path

# Path to the log CSV file
log_file = Path("energy_consumption_log.csv")


# Define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # Decode the binary message body to a string
    message = body.decode()
    print(f" [x] Received {message}")
    
    try:
        '''
          The timestamp needs to be separated for the MW. Split the message 
          based on the first space, separating the timestamp from the energy consumption
          '''
        timestamp, mw_str = message.rsplit(' ', 1)
        mw = float(mw_str)
    except ValueError as e:
        print(f"Error: Cannot parse message '{message}': {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    # Calculate the estimated energy cost
    energy_cost = mw / 1500
    # Round to 2 decimal places
    energy_cost = round(energy_cost, 2)

    # Log the data to a CSV file
    with log_file.open(mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([timestamp, mw, f"{energy_cost:.2f}"])  # Format to 2 decimal places
    
    # When done with the task, tell the user
    print(" [x] Done.")
    
    # Acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Define a main function to run the program
def main(hn: str = "localhost", qn: str = "task_queue"):
    """ Continuously listen for task messages on a named queue."""

    # When a statement can go wrong, use a try-except block
    try:
        # Try this code, if it works, keep going
        # Create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # Except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # Use the connection to create a communication channel
        channel = connection.channel()

        # Use the channel to declare a durable queue
        # A durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # Messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unacknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # Configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume(queue=qn, on_message_callback=callback)

        # Print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # Start consuming messages via the communication channel
        channel.start_consuming()

    # Except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # Initialize the log file with headers and a note if it doesn't exist
    if not log_file.exists():
        with log_file.open(mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Note: Cost of energy is $1 for every 1500 MW used"])
            writer.writerow(["Timestamp", "Energy Consumption (MW)", "Estimated Energy Cost ($)"])
    
    # Call the main function with the information needed
    main("localhost", "dayton_queue1")
