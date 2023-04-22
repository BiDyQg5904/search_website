from kafkaReader import kafkaConsumer as Reader

reader = Reader.KafkaConsumer('miniflux__entries', 'group1')

count = 0

while True:
    try:
        print(reader.read_next_message())
        count += 1
    except KeyboardInterrupt:
        break

print(count)