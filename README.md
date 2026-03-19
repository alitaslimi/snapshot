# Snapshot Fetcher

Fetches raw governance data from the [Snapshot GraphQL API](https://hub.snapshot.org/graphql) and saves it as CSV files. No data cleaning or transformation is applied — all fields are stored exactly as returned by the API.

Currently configured for the **Arbitrum DAO** (`arbitrumfoundation.eth`) space.

---

## Project Structure

```
snapshot-fetcher/
├── src/
│   ├── fetch_proposals.py       # Fetches all proposals and saves to data/proposals.csv
│   ├── fetch_votes.py           # Fetches all votes per proposal and saves to data/votes.csv
│   └── queries/
│       ├── proposals.graphql    # GraphQL query for proposals
│       └── votes.graphql        # GraphQL query for votes
├── data/
│   ├── proposals.csv            # Raw proposal data
│   └── votes.csv                # Raw vote data
├── requirements.txt
└── api.md                       # Snapshot API reference
```

---

## Setup

```bash
pip install -r requirements.txt
```

---

## Usage

Run the scripts in order — votes depend on the proposal IDs written by the first script.

**1. Fetch proposals**
```bash
python src/fetch_proposals.py
```

**2. Fetch votes**
```bash
python src/fetch_votes.py
```

---

## Output

### `data/proposals.csv`

One row per proposal. All timestamps are raw Unix integers.

| Column | Description |
|---|---|
| `id` | Proposal ID (hex string) |
| `title` | Proposal title |
| `choices` | JSON array of voting options (e.g. `["For", "Against", "Abstain"]`) |
| `start` | Voting start time (Unix timestamp) |
| `end` | Voting end time (Unix timestamp) |
| `snapshot` | Block snapshot number |
| `state` | Proposal state (`active`, `closed`, etc.) |
| `author` | Wallet address of the proposal author |
| `created` | Proposal creation time (Unix timestamp) |
| `space_id` | Space identifier (e.g. `arbitrumfoundation.eth`) |
| `space_name` | Space display name (e.g. `Arbitrum DAO`) |

### `data/votes.csv`

One row per vote. Appended incrementally — already-fetched proposals are skipped on re-runs.

| Column | Description |
|---|---|
| `proposal_id` | ID of the proposal this vote belongs to |
| `voting_time` | Vote creation time (formatted datetime) |
| `vote_id` | Unique vote ID |
| `voter` | Voter wallet address |
| `choice` | Voted choice (1-based index into the proposal's `choices` array) |
| `voting_power` | Voting power (`vp`) at time of vote |

---

## Configuration

### Adding more spaces

In `src/fetch_proposals.py`, add entries to `SPACE_IDS`:

```python
SPACE_IDS = ["arbitrumfoundation.eth", "anotherspace.eth"]
```

The `space_id` and `space_name` columns in the output will distinguish records across spaces.

### Rate limiting

The API allows 60 requests per minute. The rate limiter handles this automatically. For higher limits, see the [Snapshot API key guide](https://docs.snapshot.box/tools/api/api-keys).

---

## API

The Snapshot Hub GraphQL API endpoint is:

```
https://hub.snapshot.org/graphql
```

See `api.md` for the full API reference, or explore queries interactively at [hub.snapshot.org/graphql](https://hub.snapshot.org/graphql).
