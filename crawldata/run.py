from kafkaReader import kafkaConsumer as Reader
from clickhouse import clickhouse
from settings import *
import html_text

#
# crawl without Temporal 
# 

reader = Reader.KafkaConsumer(TOPIC_KAFKA, GROUP_ID_COUNSUMER)
orm = clickhouse.ClickHouseORM(database=DATABASE)

def get_msg() -> dict:
    msg = reader.read_next_message()
    if msg is None:
        return None
    msg['content'] = html_text.extract_text(msg['content'],guess_layout=False)
    msg['sign'] = 1
    return msg

def push_msg(data: dict) -> bool:
    return orm.insert(TABLE_NAME, data)
    
if __name__ == "__main__":
    count = 0
    while True:
        try:
            msg = get_msg()
            # print(msg)   
            if msg is not None:
                if push_msg(msg):
                    print(f"push success {msg['id']}")
                    count += 1
                else:
                    print()
            else:
                print("None")
        except KeyboardInterrupt:
            break
    print("\n" + str(count) + " message success")