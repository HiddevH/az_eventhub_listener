from azure.eventhub import EventHubConsumerClient
import os
from dotenv import load_dotenv

load_dotenv()

connection_str = os.environ["EVENT_HUB_CONN_STR"]
consumer_group = "$Default"
eventhub_name = os.environ["EVENT_HUB_NAME"]

client = EventHubConsumerClient.from_connection_string(
    connection_str, consumer_group, eventhub_name=eventhub_name
)


def on_event_batch(partition_context, events):
    partition_context.update_checkpoint()
    for e in events:
        print(f"message: \n {e.body_as_str()}")


with client:
    client.receive_batch(
        on_event_batch=on_event_batch,
        starting_position="-1",  # "-1" is from the beginning of the partition.
    )