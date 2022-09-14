# ATTENTION, this repository is slowly but steadily migrating towards the [Github Edgerun](https://github.com/edgerun) project

# Galileo Context


This project aims to provide easy to use functions to monitor a running K3S cluster, including telemetry data and
traces. The provided functions can help to build intelligent runtime optimizations, i.e., load balancing or scheduling
strategies.

For this environment to work, you need to have the following setup:

* a running K3S cluster
* redis for storing nodeinfos and as pub/sub framework
* [telemd](https://github.com/edgerun/telemd/) daemons deployed to monitor resource usage

In case you want to start a full-fledged experiment with [galileo](https://github.com/edgerun/galileo), you will also
need:

* InfluxDB to store telemetry, traces and events
* MariaDB for experiment metadata

### Example

Look for an example on how to quickly start all services under `galileocontext.cli.main`.

Environment Variables - Galileo Context
=====================

| Variable | Default | Description |
|---|---|---|
| galileo_context_logging | DEBUG | Log level (DEBUG, INFO, WARN, ERROR)
| galileo_context_telemetry_window_size | 60 | the time window that will be cached, in s | 
| galileo_context_trace_window_size | 60 | the time window that will be cached, in s | 
| galileo_context_redis_host | localhost |
| galileo_context_redis_port | 6379 |
| galileo_context_latency_graph_json| - | Latency graph json file path |

# Weighted Load Balancer Optimizer


This projects implements a python daemon that consumes runtime metric of the edgebench k3s cluster and calculates
weights for all deployed functions that are used to select nodes to serve requests.

The program is part of the osmotic-edgebench series and uses a simple heuristic, based on resource utilization of
functions.

The daemon runs periodically the weight optimization and writes the weights into the `etcd`
KV storage. Which notifies the [Go Load Balancer](https://git.dsg.tuwien.ac.at/edgebench/go-load-balancer) to adjust the
weights.

The threshold based weight optimizer can read the thresholds from environment variables. For this you need to export the
threshold for each deployment as follows:

    osmotic_lb_opt_threshold_my_function=0.3

Build
=====

The Makefile should cover all basic needs.

To setup a virtual environment, just execute:

    make venv

To build an `amd64` container for dev purposes, execute:

    make docker

To build and publish the program execute:

    make docker-release

Environment Variables - LB Opt
=====================

| Variable | Default | Description |
|---|---|---|
| osmotic_lb_opt_etcd_host | localhost | |
| osmotic_lb_opt_etcd_port | 2379 | |
| osmotic_lb_opt_expdb_influxdb_url | http://localhost:8086 | |
| osmotic_lb_opt_expdb_influxdb_token | token | |
| osmotic_lb_opt_expdb_influxdb_timeout | 10000 | in ms |
| osmotic_lb_opt_expdb_influxdb_org | org | |
| galileo_context_trace_window_size | 60 | the time window that will be cached, in s |
| galileo_context_telemetry_window_size | 60 | the time window that will be cached, in s |
| osmotic_lb_opt_reconcile_interval | 10 | determines the time between optimization runs, in s | 
| osmotic_lb_opt_etcd_cluster_id| n/a | etcd cluster id (find out with: `etcdctl member list -w fields`)
| osmotic_lb_opt_logging | DEBUG | logger level (DEBUG, INFO, WARN, ERROR) |

# Scaler/Placement Optimizer

This project implements an osmotic-pressure oriented way to solve the scaling and scheduling problem in an
threshold-based manner. The osmotic pressure is a mixture of taking several metrics into account and aggregating it to a
single value on which scaling decisions and scheduling decisions are done.

It follows an event-driven design and emits periodically monitoring information as well events in case of threshold
violations and the resulting scaling and scheduling decision. The default implementation uses Redis pub/sub and events
are explained latter.

In general, we consider clusters to be splitted into zones, that are in close proximity to each other. The daemon
observes each zone and calculates the pressure for each API Gateway of these zones.

## Monitoring events

The daemon measures periodically the pressure of each API gateway but does not emit scale/schedule decisions every time.
Therefore, it emits the calculated on a seperate channel to be able to monitor the values.

The events are publish with the following redis command:

    redis-cli  publish galileo/events <unix timestamp in seconds> api_pressure <message>

Whereas `message` is structured as follows:

```json
{
  "node": "vm5",
  "zone": "zone-a",
  "value": 93
}
```

### Other messages:

    redis-cli  publish galileo/events <unix timestamp in seconds> pressure_req <message>

```json
{
  "value": 3,
  "weight": 2,
  "utilization": 3,
  "client": "client-1",
  "fn": "fn",
  "gateway": "vm5",
  "zone": "zone-a"
}
```

     redis-cli  publish galileo/events <unix timestamp in seconds> pressure_latency_requirement <message>

```json
{
  "value": 2,
  "distance": 30,
  "client": "client-3",
  "required_latency": 10,
  "gateway": "vm5",
  "max_value": 30,
  "scaled_value": 0.33,
  "zone": "zone-a"
}
```

    redis-cli  publish galileo/events <unix timestamp in seconds> pressure_dist <message>

```json
{
  "value": 3,
  "distance": 30,
  "client": "client-3",
  "avg_distance": 5.3,
  "gateway": "vm5",
  "max_value": 30,
  "scaled_value": 0.33,
  "zone": "zone-a"
}
```

## Scale-place event

This event is only sent in case a threshold violation is detected and contains information to scale and schedule new
resources appropriately.

It is published via

        redis-cli  publish galileo/events <unix timestamp in seconds> scale_schedule <message>

Whereas `message` is structured as follows:

```json
{
  "fn": "resnet",
  "gateway": "vm5",
  "value": 0.3,
  "threshold": 0.2,
  "origin_zone": "zone-b",
  "dest_zone": "zone-a",
  "pod_creation_request": {
    "pod_name": "pod name",
    "container_name": "container name",
    "image": "docker/image:1.0",
    "labels": {
      "key-1": "value-1"
    },
    "namespace": "default"
  }
}
```

This message contains the information that the function `resnet` requires scaling because the API gateway in region
`zone-b` has violated its threshold and the result of the optimization was to schedule a new Pod to region `zone-a`.


Environment Variables - Joint-scaler/scheduler Optimizer
=====================

| Variable | Default | Description |
|---|---|---|
| schedule_scale_opt_async_pod_creation | False | Whether pods are created asynchronously or synchronous |
| schedule_scale_opt_logging | DEBUG | logger level (DEBUG, INFO, WARN, ERROR) |
| schedule_scale_opt_k8s_config | local | Whether to connect to kubernetes from a `local` process or `in_cluster` (i.e., from inside a Pod) |
| schedule_scale_opt_reconcile_interval | 10 | run optimization every... - in seconds
