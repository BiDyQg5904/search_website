from kafkaReader import kafkaConsumer as Reader
# from extract_data import extract_data_from_html as edfh 
# from parser import parser as ps 
from clickhouse import clickhouse
from settings import *

import asyncio
from datetime import timedelta
import html_text

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

# # count the number of words of a content
# def count_words(s):
#     s = s.split(" ")
#     return len(s)

# read a message from kafka that stores the information of an article
@activity.defn
async def read_data():
    reader = Reader.KafkaConsumer(TOPIC_KAFKA, GROUP_ID_COUNSUMER)
    return reader.read_next_message()

# parsing of an article
@activity.defn
async def parse_data(data):
    if data is None:
        return None
    data['content'] = html_text.extract_text(data['content'],guess_layout=False)
    data['sign'] = 1
    return data

# save data to db clickhouse
@activity.defn
async def save_data(data):
    orm = clickhouse.ClickHouseORM(database=DATABASE)
    if orm.insert(TABLE_NAME, data):
        return f"Save id {data['id']} success"
    return "Failed to save"

# workflow to define activity read_data
@workflow.defn 
class ReadWorkflow:
    @workflow.run
    async def run(self):
        return await workflow.execute_activity(
            read_data,
            start_to_close_timeout=timedelta(seconds=5),
        )

# workflow to define activity parse_data
@workflow.defn
class ParseWorkflow:
    @workflow.run
    async def run(self, data):
        return await workflow.execute_activity(
            parse_data,
            data,
            start_to_close_timeout=timedelta(seconds=5),
        )

# workflow to define activity save_data
@workflow.defn
class SaveWorkflow:
    @workflow.run   
    async def run(self, data):
        return await workflow.execute_activity(
            save_data,
            data,
            start_to_close_timeout=timedelta(seconds=5),
        )


async def main():
    client = await Client.connect("localhost:7233")

    async with Worker(
        client=client,
        task_queue="task-queue",
        workflows=[ReadWorkflow, ParseWorkflow, SaveWorkflow],
        activities=[read_data, parse_data, save_data],
    ):
        while True:
            try:
                result = await client.execute_workflow(
                ReadWorkflow.run,
                id="read-id",
                task_queue="task-queue",
                )
            
                result = await client.execute_workflow(
                    ParseWorkflow.run,
                    result,
                    id="parse-id",
                    task_queue="task-queue",
                )

                result = await client.execute_workflow(
                    SaveWorkflow.run,
                    result,
                    id="save-id",
                    task_queue="task-queue",
                )

                print(result)
            except Exception as e:
                print(f"An Error: {str(e)}")
            except KeyboardInterrupt:
                break

if __name__ == "__main__":
    asyncio.run(main())