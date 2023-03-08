# Requirements
- BGPstream to fetch BGP data: https://bgpstream.caida.org/
- Confluent-kafka to connect to Kafka: https://pypi.org/project/confluent-kafka/
- Assumes that kafka1, kafka2, and kafka3 are the name of the hosts to bootstrap connection to kafka (add `kafka1 localhost` in /etc/hosts if working locally)

# Usage
Example cron job to get data from RIS collector RRC10:
```
# get a RIB once per day
25 3 * * * python3 /home/romain/Projects/perso/kafka-toolbox/bgp/producers/bgpstream2.py -t ribs --collector rrc10
# get UPDATE messages every 15 minutes
*/15 * * * * python3 /home/romain/Projects/perso/kafka-toolbox/bgp/producers/bgpstream2.py -t updates --collector rrc10
```

# Deprecated
- bgpstream.py and bgpstream2-live.py are not used anymore
