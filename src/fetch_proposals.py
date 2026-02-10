# Libraries
import requests
import csv
import time
from datetime import datetime
from typing import List, Dict, Any
from collections import deque
import os

# Variables
API_URL = "https://hub.snapshot.org/graphql"
SPACE_ID = "arbitrumfoundation.eth"

MAX_REQUESTS_PER_MINUTE = 60
REQUEST_TIMEOUT = 30 # seconds
MAX_RETRIES = 3
RETRY_DELAY_BASE = 1 # seconds


class RateLimiter:
    """Manages API rate limiting to stay within 60 requests per minute."""
    
    def __init__(self, max_requests: int = MAX_REQUESTS_PER_MINUTE):
        self.max_requests = max_requests
        self.request_times = deque()
    
    def wait_if_needed(self):
        """Wait if we're approaching the rate limit."""
        now = time.time()
        
        # Remove timestamps older than 1 minute
        while self.request_times and now - self.request_times[0] > 60:
            self.request_times.popleft()
        
        # If we're at the limit, wait until the oldest request is more than 1 minute old
        if len(self.request_times) >= self.max_requests:
            wait_time = 60 - (now - self.request_times[0]) + 0.1  # Add small buffer
            if wait_time > 0:
                print(f"Rate limit reached. Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)
                # Clean up old timestamps after waiting
                now = time.time()
                while self.request_times and now - self.request_times[0] > 60:
                    self.request_times.popleft()
        
        # Record this request
        self.request_times.append(time.time())
    
    def get_delay(self) -> float:
        """Get recommended delay between requests."""
        if len(self.request_times) < self.max_requests:
            # Space out requests: 60 requests per minute = 1 request per second
            return 1.1  # Slightly more than 1 second to be safe
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


# GraphQL query to fetch proposals (loaded from file)
PROPOSALS_QUERY = load_query_file("proposals.graphql").replace("%SPACE%", SPACE_ID)


def make_api_request(query: str, variables: Dict[str, Any], retry_count: int = 0) -> requests.Response:
    """
    Make an API request with rate limiting, timeout, and retry logic.
    """
    rate_limiter.wait_if_needed()
    
    try:
        response = requests.post(
            API_URL,
            json={"query": query, "variables": variables},
            headers={"Content-Type": "application/json"},
            timeout=REQUEST_TIMEOUT
        )
        
        # Handle rate limiting (429 Too Many Requests)
        if response.status_code == 429:
            if retry_count < MAX_RETRIES:
                retry_delay = RETRY_DELAY_BASE * (2 ** retry_count)
                print(f"Rate limit exceeded (429). Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                return make_api_request(query, variables, retry_count + 1)
            else:
                raise Exception(f"Rate limit exceeded after {MAX_RETRIES} retries")
        
        # Handle other HTTP errors
        if response.status_code != 200:
            if retry_count < MAX_RETRIES and response.status_code >= 500:
                # Retry on server errors
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
    Fetch all proposals from Snapshot API for Arbitrum DAO.
    Handles pagination to get all proposals.
    Respects rate limits and includes retry logic.
    """
    all_proposals = []
    skip = 0
    first = 1000  # Maximum items per request
    
    while True:
        variables = {
            "first": first,
            "skip": skip
        }
        
        response = make_api_request(PROPOSALS_QUERY, variables)
        
        data = response.json()
        
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")
        
        proposals = data.get("data", {}).get("proposals", [])
        
        if not proposals:
            break
        
        all_proposals.extend(proposals)
        
        # If we got fewer than requested, we've reached the end
        if len(proposals) < first:
            break
        
        skip += first
        print(f"Fetched {len(all_proposals)} proposals so far...")
        
        # Small delay between pagination requests
        time.sleep(rate_limiter.get_delay())
    
    return all_proposals

def format_timestamp(timestamp: int) -> str:
    """Convert Unix timestamp to readable datetime string."""
    if timestamp:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    return ""

def process_proposals(proposals: List[Dict[str, Any]], vote_counts: Dict[str, int] = None) -> List[Dict[str, Any]]:
    """
    Process proposals and extract required fields.
    """
    processed = []
    
    if vote_counts is None:
        vote_counts = {}
    
    for idx, proposal in enumerate(proposals, start=1):
        proposal_id = proposal.get("id", "")
        title = proposal.get("title", "")
        created = proposal.get("created", 0)
        start = proposal.get("start", 0)
        end = proposal.get("end", 0)
        state = proposal.get("state", "")
        choices = proposal.get("choices", [])
        
        # Get total votes - use vote_counts dict if available, otherwise use scores_total as approximation
        total_votes = proposal.get("scores_total", 0)
        if total_votes == 0:
            # scores_total might not be the vote count, but it's a proxy
            # We'll use vote_counts which should be populated separately
            total_votes = 0
        
        # Format choices as string
        choices_str = ", ".join(choices) if choices else ""
        
        processed_proposal = {
            "proposal_number": idx,
            "creation_time": format_timestamp(created),
            "proposal_id": proposal_id,
            "title": title,
            "proposal_index": f"{idx} - {title}",
            "category": "",  # Category field not available in API
            "state": state,
            "voting_start_time": format_timestamp(start),
            "voting_end_time": format_timestamp(end),
            "total_votes": total_votes,
            "choices": choices_str
        }
        
        processed.append(processed_proposal)
    
    return processed


def save_to_csv(proposals: List[Dict[str, Any]], filename: str = PROPOSALS_CSV):
    """
    Save processed proposals to CSV file.
    """
    if not proposals:
        print("No proposals to save.")
        return
    
    fieldnames = [
        "proposal_number",
        "creation_time",
        "proposal_id",
        "title",
        "proposal_index",
        "category",
        "state",
        "voting_start_time",
        "voting_end_time",
        "total_votes",
        "choices"
    ]
    
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(proposals)
    
    print(f"Successfully saved {len(proposals)} proposals to {filename}")


def main():
    """
    Main function to fetch and process proposals.
    """
    print(f"Fetching proposals for {SPACE_ID}...")
    
    try:
        # Fetch all proposals
        proposals = fetch_proposals()
        print(f"Total proposals fetched: {len(proposals)}")
        
        if not proposals:
            print("No proposals found.")
            return
        
        # Fetch vote counts for all proposals
        print("Fetching vote counts...")
        proposal_ids = [p.get("id", "") for p in proposals]
        
        # Process proposals
        print("Processing proposals...")
        processed_proposals = process_proposals(proposals)
        
        # Save to CSV
        print("Saving to CSV...")
        save_to_csv(processed_proposals)
        
        print("Done!")
        
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
