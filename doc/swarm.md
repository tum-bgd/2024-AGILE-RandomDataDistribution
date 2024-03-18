# Docker Swarm

Create a Docker Swarm with a manager and several nodes.

Then checkout the repository on the manager and distribute the data folder onto each node.

Setup a registry to distribute Docker image.

```bash
docker service create --name registry --constraint node.role==manager --publish published=5000,target=5000 registry:2
```

Build the Docker image with tag `localhost:5000/pc-server` and push it to the registry.

```bash
docker build . -t localhost:5000/pc-server
docker push localhost:5000/pc-server
```

Start coordinater service on the manager node.

```bash
docker service create --name coordinator --constraint node.role==manager --env PORT=3000 --mount type=tmpfs,destination=/dev/shm,tmpfs-size=34359738368 --network host localhost:5000/pc-server
```

Create a service with some workers (adapt the `COORDINATORS` variable to point to the manager and the mount source of point to the `data` directory).

```bash
docker service create --name workers --replicas 8 --constraint node.role!=manager --mount type=bind,source=/home/setup/pce/data,destination=/app/data --env COORDINATORS=http://10.157.144.36:3000/workers --mount type=tmpfs,destination=/dev/shm,tmpfs-size=34359738368 --network host localhost:5000/pc-server
```
