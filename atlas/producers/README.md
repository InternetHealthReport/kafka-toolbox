# Requirements
- RIPE Atlas cousteau to fetch measurements information: https://pypi.org/project/ripe.atlas.cousteau/
- Confluent-kafka to connect to Kafka: https://pypi.org/project/confluent-kafka/
- Assumes that kafka1, kafka2, and kafka3 are the name of the hosts to bootstrap connection to kafka (add `kafka1 localhost` in /etc/hosts if working locally)

# Usage
Example cron job to get latest data:
```
## compute a configuration file and use it only if it contains more data than the previous configuration file
25 0 * * sun cd /home/romain/Projects/perso/kafka-toolbox/atlas/producer/; python3 getMsmIds.py > ihr-global.conf_new; ./cp_larger.sh

# Get traceroute data every 5 minutes 
*/5 * * * * python3 /home/romain/Projects/perso/kafka-toolbox/atlas/producers/traceroute.py -C /home/romain/Projects/perso/kafka-toolbox/atlas/producers/ihr-global.conf
```
