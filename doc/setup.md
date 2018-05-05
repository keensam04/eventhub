### Setup hadoop on docker

```
docker pull sequenceiq/hadoop-docker:2.7.1
docker run -p 50070:50070 -it sequenceiq/hadoop-docker:2.7.1 /etc/bootstrap.sh -bash
```
