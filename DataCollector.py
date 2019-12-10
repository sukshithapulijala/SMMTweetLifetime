from TwitterAPI import TwitterAPI, TwitterRequestError, TwitterConnectionError
import threading
import sqlite3
import time
from queue import Queue


ipc_queue = Queue()

word_set = {"Rohith,Sukshitha"}
word_set_query = "Rohith,Sukshitha"

request_statuses = 'statuses/filter'
request_trends = 'trends/place'
track = {'track': word_set_query}

new_hash_tags = "new_hash_tags"
check_trends = "check_trends"

new_day_time_period_secs = 60 * 60 * 24
new_trends_time_period_secs = 60 * 60
time_between_requests_secs = 5

current_day = "count_day0"
time_remaining_current_day_secs = 60 * 60 * 24


def time_tracker():
    global current_day
    counter = 0

    switcher = {
        "count_day0": "count_day1",
        "count_day1": "count_day2",
        "count_day2": "count_day3",
        "count_day3": "count_day4",
        "count_day4": "count_day5",
        "count_day5": "count_day6",
        "count_day6": "count_day7",
        "count_day7": "count_day0"
    }

    while True:
        current_day = switcher.get(current_day, "count_day0")
        if counter == 0:
            time.sleep(time_remaining_current_day_secs)
        else:
            time.sleep(new_day_time_period_secs)
        counter = counter + 1


def check_for_new_tweets():
    global ipc_queue

    ipc_queue.put(check_trends)
    time.sleep(new_trends_time_period_secs)


def add_tweet_to_track(word):

    global word_set_query
    global word_set

    word_set.add(word)
    word_set_query = ""

    first = True
    for x in word_set:
        if first:
            word_set_query += x
            first = False
        else:
            word_set_query += ","
            word_set_query += x

    # print(word_set_query, flush=True)


def top_tweets():
    global new_hash_tags
    global ipc_queue

    print("Running top tweets")

    # Twitter authorization keys
    consumer_key = 'm59mgTtbU2IJHDvwbbFPVeky4'
    consumer_secret = 'h70aPZuj9mCF8FaY5RmQ0THFkgSkLz87NgyYjKl8BmuKDxIVx7'
    oauth_token = '1176511751510265856-yHUdKps8q9tW7SJ3K3iGAMvaseUCBR'
    oauth_token_secret = 'iiZEZsaSHalA2vBV1fhrBwqNPI9NTWIxWaM8pvzS7s5bz'

    api = TwitterAPI(consumer_key, consumer_secret, oauth_token, oauth_token_secret)

    request = request_trends
    request_options = {'id': 1}

    response = api.request(request, request_options)
    for trend in response:
        # print(trend['name'])
        # Remove # from the name
        add_tweet_to_track(trend['name'].replace('#', ''))

    # Send a message to other thread by writing to queue
    ipc_queue.put(new_hash_tags)


def process_message():

    msg = ipc_queue.get(block=False)

    if msg is new_hash_tags:
        print("New hash tags to track")
    elif msg is check_trends:
        print("Time to check new trends")
        top_tweets()


def get_tweets_being_tracked(cursor):
    rows = cursor.execute("SELECT word FROM tweets")
    records = rows.fetchall()
    print("Words")
    for row in records:
        print(row[0])
        add_tweet_to_track(row[0])


def get_insert_row_format():

    switcher = {
        "count_day0": "INSERT INTO tweets VALUES ((?), (?), 0, 0, 0, 0, 0, 0)",
        "count_day1": "INSERT INTO tweets VALUES ((?), (?), 0, 0, 0, 0, 0, 0)",
        "count_day2": "INSERT INTO tweets VALUES ((?), 0, (?), 0, 0, 0, 0, 0)",
        "count_day3": "INSERT INTO tweets VALUES ((?), 0, 0, (?), 0, 0, 0, 0)",
        "count_day4": "INSERT INTO tweets VALUES ((?), 0, 0, 0, (?), 0, 0, 0)",
        "count_day5": "INSERT INTO tweets VALUES ((?), 0, 0, 0, 0, (?), 0, 0)",
        "count_day6": "INSERT INTO tweets VALUES ((?), 0, 0, 0, 0, 0, (?), 0)",
        "count_day7": "INSERT INTO tweets VALUES ((?), 0, 0, 0, 0, 0, 0, (?))"
    }

    return switcher.get(current_day, "INSERT INTO tweets VALUES ((?), (?), 0, 0, 0, 0, 0, 0)")


def tweet_counter():
    global word_set_query
    global word_set
    global track

    # Twitter authorization keys
    consumer_key = 'm59mgTtbU2IJHDvwbbFPVeky4'
    consumer_secret = 'h70aPZuj9mCF8FaY5RmQ0THFkgSkLz87NgyYjKl8BmuKDxIVx7'
    oauth_token = '1176511751510265856-yHUdKps8q9tW7SJ3K3iGAMvaseUCBR'
    oauth_token_secret = 'iiZEZsaSHalA2vBV1fhrBwqNPI9NTWIxWaM8pvzS7s5bz'

    api = TwitterAPI(consumer_key, consumer_secret, oauth_token, oauth_token_secret)

    while True:
        # DB connection to store data
        conn = sqlite3.connect('twitter.db', check_same_thread=False)
        cursor = conn.cursor()

        track = {'track': word_set_query}
        print(track)

        request = request_statuses
        request_options = track

        msg_in_queue = False
        sent_request = False

        if ipc_queue.qsize() > 0:
            msg_in_queue = True

        if msg_in_queue is False:
            r = api.request(request, request_options)
            sent_request = True
        else:
            process_message()

        if sent_request:
            try:
                for item in r.get_iterator() or ipc_queue.qsize() > 0:

                    if ipc_queue.qsize() > 0:
                        try:
                            r.close()
                            print("Msg in queue. Will sleep for a few secs to avoid error 420")
                            time.sleep(time_between_requests_secs)
                            process_message()
                            break
                        except:
                            print("Empty queue")

                    if 'text' in item:
                        print(item, flush=True)

                        for word in word_set:
                            if str(word) in item['text']:
                                rows = cursor.execute("SELECT * FROM tweets WHERE word like (?)", (str(word),))
                                non_zero_rows = rows.fetchone()
                                if non_zero_rows:
                                    cursor.execute("UPDATE tweets SET {0} = {1} + 1 WHERE word LIKE \"{2}\"".
                                                   format(str(current_day), str(current_day), word))

                                else:
                                    insert_row = get_insert_row_format()
                                    cursor.execute(insert_row, (str(word), 1,))

                        conn.commit()
                    elif 'limit' in item:
                        skip = item['limit'].get('track')
                        print('*** SKIPPED %d TWEETS' % skip)
                    elif 'disconnect' in item:
                        print('[disconnect] %s' % item['disconnect'].get('reason'))
                        cursor.close()
                        conn.close()
                        break

            except TwitterRequestError as e:
                if e.status_code < 500:
                    print("Something critical failed")
                else:
                    print("Temporary failure")
                    pass
                time.sleep(time_between_requests_secs)

            except TwitterConnectionError:
                print("Connection error")
                time.sleep(time_between_requests_secs)

                cursor.close()
                conn.close()


if __name__ == "__main__":

    # DB connection to store data
    conn_main = sqlite3.connect('twitter.db', check_same_thread=False)
    cursor_main = conn_main.cursor()
    cursor_main.execute('CREATE TABLE IF NOT EXISTS tweets('
                        'word TEXT PRIMARY KEY'
                        ', count_day1 REAL'
                        ', count_day2 REAL'
                        ', count_day3 REAL'
                        ', count_day4 REAL'
                        ', count_day5 REAL'
                        ', count_day6 REAL'
                        ', count_day7 REAL'
                        ')'
                        )

    get_tweets_being_tracked(cursor_main)

    t1 = threading.Thread(target=time_tracker)
    t2 = threading.Thread(target=check_for_new_tweets)
    t3 = threading.Thread(target=tweet_counter)

    print("Started thread time_tracker()", flush=True)
    t1.start()

    time.sleep(2)
    print("Started thread check_trends()", flush=True)
    t2.start()

    time.sleep(2)

    print("Started thread tweet_counter()", flush=True)
    t3.start()

    t1.join()
    t2.join()
    t3.join()
