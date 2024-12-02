from atproto import FirehoseSubscribeLabelsClient
import asyncio


async def firehose_listener():
    # Initialize the client
    client = FirehoseSubscribeLabelsClient()

    # Log in with your credentials (replace with your actual credentials)
    # client.login(identifier="your_username_or_email", password="your_password")

    # Open the firehose connection
    async for message in client.firehose():
        # Handle each message from the firehose
        print(message)

# Run the listener
asyncio.run(firehose_listener())
