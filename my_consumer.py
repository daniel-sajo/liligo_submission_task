# imports
import threading, time
from kafka import KafkaConsumer
import json

class Consumer(threading.Thread):
    daemon = False

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='latest')
        consumer.subscribe(['test'])

        time_interval_delta = 10000
        # timestamp in milliseconds

        time_interval_start = -100
        # time_interval_end = time_interval_start + time_interval_delta

        average_sum = 0
        average_num_of_elements = 0

        for message in consumer:
            if time_interval_start == -100:
                time_interval_start = message.timestamp
                time_interval_end = time_interval_start+time_interval_delta

            # print(time_interval_start, time_interval_end, message.timestamp)

            if message.timestamp>=time_interval_start and message.timestamp<time_interval_end:
                # compute average
                average_num_of_elements += 1

                msg_val_json = json.loads(message.value.decode('ascii'))
                # response.readall().decode('utf-8')

                average_sum += msg_val_json['number']

            else:
                if average_num_of_elements>0:
                    print(7*'-','\n', 'average:', average_sum/average_num_of_elements)
                else:
                    print(7 * '-', '\n', 'average:', 'NaN')

                average_sum = 0
                average_num_of_elements = 0

                average_num_of_elements = 1

                msg_val_json = json.loads(message.value.decode('ascii'))
                average_sum += msg_val_json['number']

                time_interval_start = message.timestamp
                time_interval_end = time_interval_start + time_interval_delta


def main():
    threads = [Consumer()]

    for t in threads:
        t.start()

    # time.sleep(30)

main()