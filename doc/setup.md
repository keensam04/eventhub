### Setup hadoop on docker

```
docker pull sequenceiq/hadoop-docker:2.7.1
docker run -p 50070:50070 -it sequenceiq/hadoop-docker:2.7.1 /etc/bootstrap.sh -bash
```

### MySQL

```
create database eventhub;
use eventhub;
create table segments (source nvarchar(32) not null, epoch_from bigint not null, epoch_to bigint not null, last_updated timestamp not null default CURRENT_TIMESTAMP);
```

### Zookeeper
**version used: 3.4.10**

```
C:\kafka\kafka_2.11-0.11.0.1\bin\windows\zookeeper-server-start.bat C:\kafka\kafka_2.11-0.11.0.1\config\zookeeper.properties
```
