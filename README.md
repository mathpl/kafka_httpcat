# kafka_httpcat

kafkacat -C -b localhost:9092 -C -t topic -o stored -p1 -u | kafka_httpcat -c http_out.toml
