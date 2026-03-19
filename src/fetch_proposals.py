# Libraries
import requests
import csv
import json
import time
from typing import List, Dict, Any
from collections import deque
import os

# Variables
API_URL = "https://hub.snapshot.org/graphql"
SPACE_IDS = ["arbitrumfoundation.eth"]

MAX_REQUESTS_PER_MINUTE = 60
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
RETRY_DELAY_BASE = 1  # seconds


class RateLimiter:
    """Manages API rate limiting to stay within 60 requests per minute."""

    def __init__(self, max_requests: int = MAX_REQUESTS_PER_MINUTE):
        self.max_requests = max_requests
        self.request_times = deque()

    def wait_if_needed(self):
        """Wait if we're approaching the rate limit."""
        now = time.time()

        while self.request_times and now - self.request_times[0] > 60:
            self.request_times.popleft()

        if len(self.request_times) >= self.max_requests:
            wait_time = 60 - (now - self.request_times[0]) + 0.1
            if wait_time > 0:
                print(f"Rate limit reached. Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)
                now = time.time()
                while self.request_times and now - self.request_times[0] > 60:
                    self.request_times.popleft()

        self.request_times.append(time.time())

    def get_delay(self) -> float:
        if len(self.request_times) < self.max_requests:
            return 1.1
        return 0


# Global rate limiter instance
rate_limiter = RateLimiter()

# Paths
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
PROPOSALS_CSV = os.path.join(DATA_DIR, "proposals.csv")


def load_query_file(name: str) -> str:
    path = os.path.join(os.path.dirname(__file__), "queries", name)
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


PROPOSALS_QUERY = load_query_file("proposals.graphql")


def make_api_request(query: str, variables: Dict[str, Any], retry_count: int = 0) -> requests.Response:
    """Make an API request with rate limiting, timeout, and retry logic."""
    rate_limiter.wait_if_needed()

    try:
        response = requests.post(
            API_URL,
            json={"query": query, "variables": variables},
            headers={"Content-Type": "application/json"},
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code == 429:
            if retry_count < MAX_RETRIES:
                retry_delay = RETRY_DELAY_BASE * (2 ** retry_count)
                print(f"Rate limit exceeded (429). Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                return make_api_request(query, variables, retry_count + 1)
            else:
                raise Exception(f"Rate limit exceeded after {MAX_RETRIES} retries")

        if response.status_code != 200:
            if retry_count < MAX_RETRIES and response.status_code >= 500:
                retry_delay = RETRY_DELAY_BASE * (2 ** retry_count)
                print(f"Server error {response.status_code}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                return make_api_request(query, variables, retry_count + 1)
            else:
                raise Exception(f"API request failed with status {response.status_code}: {response.text}")

        return response

    except requests.exceptions.Timeout:
        if retry_count < MAX_RETRIES:
            retry_delay = RETRY_DELAY_BASE * (2 ** retry_count)
            print(f"Request timeout. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            return make_api_request(query, variables, retry_count + 1)
        else:
            raise Exception(f"Request timeout after {MAX_RETRIES} retries")

    except requests.exceptions.RequestException as e:
        if retry_count < MAX_RETRIES:
            retry_delay = RETRY_DELAY_BASE * (2 ** retry_count)
            print(f"Request error: {e}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            return make_api_request(query, variables, retry_count + 1)
        else:
            raise


def fetch_proposals() -> List[Dict[str, Any]]:
    """
    Fetch all proposals from the Snapshot API.
    Handles pagination, rate limiting, and retries.
    """
    all_proposals = []
    skip = 0
    first = 1000

    while True:
        variables = {
            "first": first,
            "skip": skip,
            "spaces": SPACE_IDS
        }

        response = make_api_request(PROPOSALS_QUERY, variables)
        data = response.json()

        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")

        proposals = data.get("data", {}).get("proposals", [])

        if not proposals:
            break

        all_proposals.extend(proposals)

        if len(proposals) < first:
            break

        skip += first
        print(f"Fetched {len(all_proposals)} proposals so far...")

        time.sleep(rate_limiter.get_delay())

    return all_proposals


def save_to_csv(proposals: List[Dict[str, Any]], filename: str = PROPOSALS_CSV):
    """Save raw proposals to CSV, flattening the nested space object."""
    if not proposals:
        print("No proposals to save.")
        return

    fieldnames = [
        "id",
        "title",
        "choices",
        "start",
        "end",
        "snapshot",
        "state",
        "author",
        "created",
        "space_id",
        "space_name"
    ]

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for proposal in proposals:
            space = proposal.get("space") or {}
            writer.writerow({
                "id": proposal.get("id", ""),
                "title": proposal.get("title", ""),
                "choices": json.dumps(proposal.get("choices", [])),
                "start": proposal.get("start", ""),
                "end": proposal.get("end", ""),
                "snapshot": proposal.get("snapshot", ""),
                "state": proposal.get("state", ""),
                "author": proposal.get("author", ""),
                "created": proposal.get("created", ""),
                "space_id": space.get("id", ""),
                "space_name": space.get("name", "")
            })

    print(f"Successfully saved {len(proposals)} proposals to {filename}")


def main():
    print(f"Fetching proposals for spaces: {SPACE_IDS}...")

    try:
        proposals = fetch_proposals()
        print(f"Total proposals fetched: {len(proposals)}")

        if not proposals:
            print("No proposals found.")
            return

        save_to_csv(proposals)
        print("Done!")

    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
