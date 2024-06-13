"""
    This program sends data from dayton_energy_consumption.csv to a queue on
    the RabbitMQ server. To simulate work there will be a sleep delay of 
    60 seconds.    

    Author: Laura Dooley
    Date: June 12, 2024
"""

import pika
import sys
import webbrowser
import time
import datetime
import pandas as pd
from pathlib import Path


def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()


def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()


def send_messages_from_csv(host: str, queue_name: str, csv_file: Path):
    """
    Read messages from a CSV file and send each message to the RabbitMQ queue.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        csv_file (Path): the path to the CSV file containing messages
    """
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file)

        # Check if the DataFrame is empty
        if df.empty:
            print("Error: CSV file is empty")
            return

        # Use the first column for messages regardless of its name
        energy_consumption = df.columns[1]

        # Iterate over each row in the DataFrame and send the message
        for index, row in df.iterrows():
            try:
                message = row[energy_consumption]
                # include a timestamp with each message
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                full_message = f'{timestamp} {message}'
                send_message(host, queue_name, str(full_message))
                # sleep for 5 seconds to simulate work
                time.sleep(5)
            except Exception as e:
                print(f"Error processing row {index}: {e}")
    
    except Exception as e:
        print(f"Error reading CSV file: {e}")

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below

if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()

    # Set the CSV file path
    csv_file = Path("dayton_energy_consumption.csv")

    # Send the messages from the CSV file to the queue
    send_messages_from_csv("localhost", "dayton_queue1", csv_file)
