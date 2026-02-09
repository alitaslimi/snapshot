import requests
import csv
import time
from datetime import datetime
from typing import List, Dict, Any, Set
from collections import deque
import os

# Snapshot GraphQL API endpoint
API_URL = "https://hub.snapshot.org/graphql"

# Space filter for Arbitrum DAO
SPACE_ID = "arbitrumfoundation.eth"

# API Rate Limits
MAX_REQUESTS_PER_MINUTE = 60
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2  # seconds


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

# GraphQL query to fetch votes for a specific proposal
VOTES_QUERY = """
query GetVotes($first: Int!, $skip: Int!, $proposal: String!) {
  votes (
    first: $first,
    skip: $skip,
    where: {
      proposal: $proposal
    },
    orderBy: "created",
    orderDirection: asc
  ) {
    id
    voter
    created
    choice
    vp
    proposal {
      id
    }
  }
}
"""


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


def get_already_fetched_proposals() -> Set[str]:
    """
    Read votes.csv and return the set of proposal_ids that have already been fetched.
    Returns an empty set if the file doesn't exist.
    """
    if not os.path.exists("votes.csv"):
        return set()
    
    fetched_proposals = set()
    try:
        with open("votes.csv", "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                proposal_id = row.get("proposal_id", "").strip()
                if proposal_id:
                    fetched_proposals.add(proposal_id)
    except Exception as e:
        print(f"Warning: Could not read existing votes.csv: {e}")
    
    return fetched_proposals


def read_proposals() -> List[Dict[str, Any]]:
    """
    Read proposal_ids from proposals.csv file.
    """
    proposals = []
    
    if not os.path.exists("proposals.csv"):
        raise FileNotFoundError("proposals.csv not found. Please run fetch_proposals.py first.")
    
    try:
        with open("proposals.csv", "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                proposal_id = row.get("proposal_id", "").strip()
                if proposal_id:
                    proposals.append({
                        "proposal_id": proposal_id,
                        "title": row.get("title", "")
                    })
    except Exception as e:
        raise Exception(f"Error reading proposals.csv: {e}")
    
    return proposals


def fetch_votes_for_proposal(proposal_id: str) -> List[Dict[str, Any]]:
    """
    Fetch all votes for a specific proposal.
    Handles pagination to get all votes.
    """
    all_votes = []
    skip = 0
    first = 1000  # Maximum items per request
    
    while True:
        variables = {
            "first": first,
            "skip": skip,
            "proposal": proposal_id
        }
        
        response = make_api_request(VOTES_QUERY, variables)
        
        data = response.json()
        
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")
        
        votes = data.get("data", {}).get("votes", [])
        
        if not votes:
            break
        
        all_votes.extend(votes)
        
        # If we got fewer than requested, we've reached the end
        if len(votes) < first:
            break
        
        skip += first
        print(f"  Fetched {len(all_votes)} votes so far...")
        
        # Small delay between pagination requests
        time.sleep(rate_limiter.get_delay())
    
    return all_votes


def format_timestamp(timestamp: int) -> str:
    """Convert Unix timestamp to readable datetime string."""
    if timestamp:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    return ""


def process_votes(votes: List[Dict[str, Any]], proposal_id: str) -> List[Dict[str, Any]]:
    """
    Process votes and extract required fields:
    proposal_id, created (as voting_time), id (as vote_id), voter, choice, vp (as voting_power)
    """
    processed = []
    
    for vote in votes:
        processed_vote = {
            "proposal_id": proposal_id,
            "voting_time": format_timestamp(vote.get("created", 0)),
            "vote_id": vote.get("id", ""),
            "voter": vote.get("voter", ""),
            "choice": vote.get("choice", ""),
            "voting_power": vote.get("vp", "")
        }
        processed.append(processed_vote)
    
    return processed


def append_to_csv(votes: List[Dict[str, Any]], filename: str = "votes.csv"):
    """
    Append votes to CSV file. Creates the file if it doesn't exist.
    """
    if not votes:
        return
    
    fieldnames = [
        "proposal_id",
        "voting_time",
        "vote_id",
        "voter",
        "choice",
        "voting_power"
    ]
    
    file_exists = os.path.exists(filename)
    
    try:
        with open(filename, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header only if file is new
            if not file_exists:
                writer.writeheader()
            
            writer.writerows(votes)
        
        print(f"Appended {len(votes)} votes to {filename}")
    except Exception as e:
        raise Exception(f"Error writing to {filename}: {e}")


def main():
    """
    Main function to fetch and process votes.
    """
    print(f"Starting vote fetch for {SPACE_ID}...\n")
    
    try:
        # Read proposals from CSV
        print("Reading proposals from proposals.csv...")
        proposals = read_proposals()
        print(f"Found {len(proposals)} proposals\n")
        
        # Get set of already fetched proposals
        print("Checking which proposals have already been processed...")
        already_fetched = get_already_fetched_proposals()
        print(f"Found {len(already_fetched)} proposals already fetched\n")
        
        # Filter proposals that need to be fetched
        proposals_to_fetch = [p for p in proposals if p["proposal_id"] not in already_fetched]
        
        if not proposals_to_fetch:
            print("All proposals have already been processed. No new votes to fetch.")
            return
        
        print(f"Need to fetch votes for {len(proposals_to_fetch)} proposals\n")
        
        # Fetch votes for each proposal
        total_votes_added = 0
        
        for idx, proposal in enumerate(proposals_to_fetch, start=1):
            proposal_id = proposal["proposal_id"]
            title = proposal["title"]
            
            print(f"[{idx}/{len(proposals_to_fetch)}] Fetching votes for: {title}")
            print(f"  Proposal ID: {proposal_id}")
            
            try:
                # Fetch votes for this proposal
                votes = fetch_votes_for_proposal(proposal_id)
                print(f"  Total votes fetched: {len(votes)}")
                
                if votes:
                    # Process votes
                    processed_votes = process_votes(votes, proposal_id)
                    
                    # Append to CSV
                    append_to_csv(processed_votes, "votes.csv")
                    total_votes_added += len(processed_votes)
                else:
                    print(f"  No votes found for this proposal")
                
                print()  # Blank line for readability
                
            except Exception as e:
                print(f"  Error fetching votes for proposal {proposal_id}: {e}")
                print()  # Blank line for readability
                continue
        
        print(f"\nDone! Total votes added: {total_votes_added}")
        
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
