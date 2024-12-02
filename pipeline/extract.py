from atproto_firehose import FirehoseSubscribeReposClient, parse_subscribe_repos_message
import ssl
import certifi
from atproto import CAR, models
from atproto_client.models.utils import get_or_create
import json


class JSONExtra(json.JSONEncoder):
    """Serializes raw objects (including CID-Content Identifier) as strings."""

    def default(self, obj: bytes):
        try:
            result = json.JSONEncoder.default(self, obj)
            return result
        except:
            return repr(obj)


def get_firehose_data(message) -> list[str]:
    """"Function handles incoming messages from the BlueSky firehose, parses the raw data 
    into json objects and returns the message posted."""
    topics = ['building', 'lake']

    commit = parse_subscribe_repos_message(message)
    if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
        return
    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        if op.action in ["create"] and op.cid:
            raw = car.blocks.get(op.cid)
            cooked = get_or_create(raw, strict=False)
            if cooked.py_type == "app.bsky.feed.post":

                json_data = json.dumps(raw, cls=JSONExtra, indent=2)
                parsed_json = json.loads(json_data)
                firehose_text = parsed_json['text']
                if any(ext in firehose_text for ext in topics):
                    print(firehose_text)
                    print("="*100)


def bluesky_firehose_connection() -> None:
    """Function to connect to BlueSky Firehose API using a secure SSL context to ensure
    secure communication."""
    # Create an SSL context that skips verification
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    client = FirehoseSubscribeReposClient()

    # Modify the client connection to use this unverified context
    # Assuming the library allows this (check docs)
    client.ssl_context = ssl_context

    client.start(get_firehose_data)


if __name__ == "__main__":
    topics = ['blue', 'red', 'chocolate']
    bluesky_firehose_connection()
