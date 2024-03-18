# %%
import pyarrow
import requests
import time

ENDPOINT = "http://0.0.0.0:3000"
# ENDPOINT = "http://10.157.144.36:3000"  # may need to adapt

NUM_POINTS = 149676342  # AHN3/C_69AZ1.LAZ
# NUM_POINTS = 197181263  # AHN4/C_69AZ1.LAZ


def get_timed(url):
    """GET url and return wall time."""
    start = time.time()
    response = requests.get(url)
    response.raise_for_status()
    return time.time() - start


def query_timed(url):
    """GET query url and return stats."""
    start = time.time()
    response = requests.get(url)
    response.raise_for_status()
    elapsed_raw = time.time() - start

    loaded_array = pyarrow.ipc.open_stream(response.content).read_all()
    elapsed = time.time() - start
    return len(loaded_array), elapsed, elapsed_raw


# -----------------------------------------------------------------------------
# Loading, Indexing & Query Benchmark (in memory)
# -----------------------------------------------------------------------------
# Follow the README.md to setup a server (restart to create clean setup)

# Load the C_69AZ1.LAZ file from AHN3
# %%
url = f"{ENDPOINT}/load?uris=./data/AHN3/C_69AZ1.LAZ&delta=500,500,2000,0.125"

elapsed = get_timed(url)
throughput = 149676342 / elapsed / 1000000

print(f"Executed `{url}` in {elapsed:.3f} seconds")
print(f"Load throughput: {throughput:.3f} million points per second")

# Inspect memory usage
# find the `process pid` of the running server then run
# $ sudo pmap <process pid>

# Index
# %%
url = f"{ENDPOINT}/index"
elapsed = get_timed(url)

print(f"Executed `{url}` in {elapsed:.3f} seconds")

# Inspect memory usage
# find the `process pid` of the running server then run
# $ sudo pmap <process pid>

# Box query
# %%
url = f"{ENDPOINT}/points?bounds=174000,315000,0,0,174060,315060,1000,1"
print(f"Query: `{url}`")

num_points, elapsed, elapsed_raw = query_timed(url)
print(f"Queried {num_points} points in {elapsed:.3f} ({elapsed_raw:.3f}) seconds")

# Sampling query
url = f"{ENDPOINT}/points?p=0.0005"
print(f"Query: `{url}`")

num_points, elapsed, elapsed_raw = query_timed(url)
print(f"Queried {num_points} points in {elapsed:.3f} ({elapsed_raw:.3f}) seconds")

# -----------------------------------------------------------------------------
# Loading, Indexing & Query Benchmark (on disk)
# -----------------------------------------------------------------------------
# Follow the README.md to setup a server (restart to create clean setup)

# Load the C_69AZ1.LAZ file from AHN3
# %%
url = f"{ENDPOINT}/load?uris=./data/AHN3/C_69AZ1.LAZ&delta=500,500,2000,0.125&store=./data/tmp"

elapsed = get_timed(url)
throughput = 149676342 / elapsed / 1000000

print(f"Executed `{url}` in {elapsed:.3f} seconds")
print(f"Load throughput: {throughput:.3f} million points per second")

# Inspect storage footprint
# $ du -sh data/tmp

# Index
# %%
url = f"{ENDPOINT}/index"
elapsed = get_timed(url)

print(f"Executed `{url}` in {elapsed:.3f} seconds")

# Box query
# %%
url = f"{ENDPOINT}/points?bounds=174000,315000,0,0,174060,315060,1000,1"
print(f"Query: `{url}`")

num_points, elapsed, elapsed_raw = query_timed(url)
print(f"Queried {num_points} points in {elapsed:.3f} ({elapsed_raw:.3f}) seconds")

# Sampling query
url = f"{ENDPOINT}/points?p=0.0005"
print(f"Query: `{url}`")

num_points, elapsed, elapsed_raw = query_timed(url)
print(f"Queried {num_points} points in {elapsed:.3f} ({elapsed_raw:.3f}) seconds")

# -----------------------------------------------------------------------------
# Loading and Indexing Benchmark (Potree Converter)
# -----------------------------------------------------------------------------
# With the Potree Converter release binary in ./PotreeConverter
# $ LD_LIBRARY_PATH=PotreeConverter/ PotreeConverter/PotreeConverter -i ./data/AHN3/C_69AZ1.LAZ -o ./data/potree

# -----------------------------------------------------------------------------
# Query scaling plot
# -----------------------------------------------------------------------------
# Setup a swarm with 8 nodes and distribute the following files from AHN4:
# C_69AZ1 C_69AZ2 C_69BZ1 C_69AN1 C_69AN2 C_69BN1 C_69CN2 C_69DN1
# %%
ENDPOINT = "http://10.157.144.36:3000"  # may need to adapt

# NUM_POINTS = 2514347929 # AHN3 extract of 6 files
NUM_POINTS = 4498801857  # AHN4 extract of 8 files

# Load the data
# %%
url = f"{ENDPOINT}/load?uris=./data/AHN4/C_69AN1.LAZ,./data/AHN4/C_69AN2.LAZ,./data/AHN4/C_69AZ1.LAZ,./data/AHN4/C_69AZ2.LAZ,./data/AHN4/C_69BN1.LAZ,./data/AHN4/C_69BZ1.LAZ,./data/AHN4/C_69CN2.LAZ,./data/AHN4/C_69DN1.LAZ&delta=1000,1000,2000,0.125"
elapsed = get_timed(url)
throughput = 149676342 / elapsed / 1000000

print(f"Executed `{url}` in {elapsed:.3f} seconds")
print(f"Load throughput: {throughput:.3f} million points per second")

# Index
# %%
start = time.time()
response = requests.get(f"{ENDPOINT}/index")
response.raise_for_status()
elapsed = time.time() - start

print(f"Executed `{url}` in {elapsed:.3f} seconds")

# Queries
# %%
POINT_QUERIES = [
    # "points?p=0.000005",
    "points?p=0.00001",
    "points?p=0.00005",
    "points?p=0.0001",
    # "points?p=0.0002",
    "points?p=0.0005",
    "points?p=0.001",
    "points?bounds=174000,315000,0,0,174060,315060,1000,1",
    "points?bounds=174000,315000,0,0,174120,315120,1000,1",
    "points?bounds=174000,315000,0,0,174240,315240,1000,1",
    "points?bounds=174000,315000,0,0,174480,315480,1000,1",
]

for query in POINT_QUERIES:
    url = f"{ENDPOINT}/{query}"
    print(f"Query: `{url}`")

    num_points, elapsed, elapsed_raw = query_timed(url)

    print(f"Queried {num_points} points in {elapsed:.3f} ({elapsed_raw:.3f}) seconds")


# Plot
# measurements in the dscale dictionary are collected from the docker logs of the nodes (worker)
# and manager/coordinator (server) and this script output.

# %%
import matplotlib.pyplot as plt

dscale = {
    "boxpoints": [59259, 239005, 1014793, 4965529],
    "ppoints": [45304, 224922, 450603, 2250780, 4502368],
    "boxquery": [0.166, 0.313, 1.153, 4.815],
    "pquery": [0.599, 0.639, 0.874, 2.612, 4.875],
    "boxquery_server": [0.102, 0.193, 0.677, 2.481],
    "pquery_server": [0.320, 0.496, 0.632, 1.536, 2.771],
    "boxquery_worker": [0.067, 0.069, 0.078, 0.107],
    "pquery_worker": [0.232, 0.375, 0.387, 0.404, 0.434],
}

fig, ax = plt.subplots()

color = "tab:red"
ax.set_xlabel("number of points (n)")
ax.set_ylabel("query response time (s)")
# api response time
ax.plot(
    dscale["boxpoints"],
    dscale["boxquery"],
    color=color,
    linestyle="dashed",
    marker="s",
    label="box query (api)",
)
ax.plot(
    dscale["ppoints"],
    dscale["pquery"],
    color=color,
    linestyle="dashed",
    marker="^",
    label="importance query (api)",
)
# server response time
ax.plot(
    dscale["boxpoints"],
    dscale["boxquery_server"],
    color=color,
    linestyle="dashed",
    marker="s",
    alpha=0.75,
    label="box query (server)",
)
ax.plot(
    dscale["ppoints"],
    dscale["pquery_server"],
    color=color,
    linestyle="dashed",
    marker="^",
    alpha=0.75,
    label="importance query (server)",
)
# worker response time
ax.plot(
    dscale["boxpoints"],
    dscale["boxquery_worker"],
    color=color,
    linestyle="dashed",
    marker="s",
    alpha=0.5,
    label="box query (worker)",
)
ax.plot(
    dscale["ppoints"],
    dscale["pquery_worker"],
    color=color,
    linestyle="dashed",
    marker="^",
    alpha=0.5,
    label="importance query (worker)",
)

ax.set_ylim(bottom=0)
ax.tick_params(axis="y")
ax.legend(loc="upper left", frameon=False)

plt.show()


# -----------------------------------------------------------------------------
# Worker scaling plot
# -----------------------------------------------------------------------------
# Setup a Docker swarm with 1, 2, 4 and 8 nodes, load the C_69AZ1.LAZ file 
# from AHN3 and execute the .../points?bounds=174000,315000,0,0,174060,315060,1000,1 
# and .../points?p=0.0005 to collect the measures in `wscale` from the log.
# %%
import matplotlib.pyplot as plt

wscale = {
    "workers": [1, 2, 4, 8],
    "load": [69.7, 72.8, 73.3, 70.0],
    "boxquery": [0.274, 0.268, 0.250, 0.254],
    "pquery": [0.765, 0.526, 0.416, 0.318],
}

fig, ax = plt.subplots()

color = "tab:red"
ax.set_xlabel("number of workers (n)")
ax.set_ylabel("query response time (s)")
ax.plot(
    wscale["workers"],
    wscale["boxquery"],
    color=color,
    linestyle="dashed",
    marker="s",
    label="box query (3d)",
)
ax.plot(
    wscale["workers"],
    wscale["pquery"],
    color=color,
    linestyle="dashed",
    marker="^",
    label="importance query (1d)",
)
ax.tick_params(axis="y")
ax.legend(loc="upper right", frameon=False)

plt.show()
