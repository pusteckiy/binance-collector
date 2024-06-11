import websocket
import json
import time
import yaml
import requests
import threading
import sys

from database import (
    connect_db,
    is_records_older_than_n_seconds,
    save_to_database,
    get_average_volume,
)
from utils import (
    send_telegram_message,
    calculate_prices_and_volumes,
)

order_book = {"bids": {}, "asks": {}}
event_buffer = []
last_update_id = None


with open("config.yaml", "r") as stream:
    config = yaml.safe_load(stream)


def connect_to_stream(pair: str):
    global last_update_id
    global event_buffer

    url = f"wss://stream.binance.com:9443/ws/{pair.lower()}@depth@1000ms"

    def on_message(ws, message):
        message_json = json.loads(message)
        handle_message(message_json)
        (
            mid_price,
            price_above,
            price_below,
            volume_above,
            volume_below,
        ) = calculate_prices_and_volumes(order_book)

        conn = connect_db()
        save_to_database(conn, pair, mid_price, volume_above, volume_below)
        avg_volume_above, avg_volume_below = get_average_volume(conn, pair)
        if is_records_older_than_n_seconds(conn, pair, config["monitoring"]["period"]):
            deviation_above = abs(volume_above - avg_volume_above) / avg_volume_above
            deviaton_below = abs(volume_below - avg_volume_below) / avg_volume_below

            if deviation_above > config["monitoring"]["deviation"]:
                response_message = (
                    f"🌐 Anomaly volume ABOVE ({deviation_above * 100:.0f}%) for {pair.upper()}\n├ Price: {mid_price:.2f}\n├ Price (+{config['monitoring']['distance'] * 100}%): {price_above:.2f}\n├ Price (-{config['monitoring']['distance'] * 100}%): {price_below:.2f}\n"
                    f"├ Volume Above: {volume_above:.2f}\n├ Volume Below: {volume_below:.2f}\n├ Average Volume Above ({config['monitoring']['period']} sec.): {avg_volume_above:.2f}\n└ Average Volume Below ({config['monitoring']['period']} sec.): {avg_volume_below:.2f}"
                )
                send_telegram_message(response_message)
            elif deviaton_below > config["monitoring"]["deviation"]:
                response_message = (
                    f"🌐 Anomaly volume BELOW ({deviaton_below * 100:.0f}%) for {pair.upper()}\n├ Price: {mid_price:.2f}\n├ Price (+{config['monitoring']['distance'] * 100}%): {price_above:.2f}\n├ Price (-{config['monitoring']['distance'] * 100}%): {price_below:.2f}\n"
                    f"├ Volume Above: {volume_above:.2f}\n├ Volume Below: {volume_below:.2f}\n├ Average Volume Above ({config['monitoring']['period']} sec.): {avg_volume_above:.2f}\n└ Average Volume Below ({config['monitoring']['period']} sec.): {avg_volume_below:.2f}"
                )
                send_telegram_message(response_message)
        conn.close()

    def on_error(ws, error):
        print("ERROR:", error)
        sys.exit()

    def on_close(ws, close_status_code, close_msg):
        print("Connection closed")

    ws = websocket.WebSocketApp(
        url, on_message=on_message, on_error=on_error, on_close=on_close
    )
    ws.run_forever()


def get_depth_snapshot(pair: str):
    response = requests.get(
        config["binance"]["rest_url"], params={"symbol": pair.upper(), "limit": 1000}
    )
    return response.json()


def handle_message(message):
    global last_update_id
    global event_buffer

    if last_update_id is None:
        event_buffer.append(message)
    else:
        if message["U"] <= last_update_id + 1 and message["u"] >= last_update_id + 1:
            apply_event(message)
            last_update_id = message["u"]
        else:
            event_buffer.append(message)
            print("event buffer in handle")
            while event_buffer:
                print("working with events")
                apply_event(event_buffer.pop(0))
                last_update_id = event_buffer[0]["u"]


def sync_events(depth_snapshot):
    global last_update_id
    global event_buffer

    last_update_id = depth_snapshot["lastUpdateId"]

    filtered_events = [event for event in event_buffer if event["u"] > last_update_id]
    event_buffer.clear()

    for event in filtered_events:
        apply_event(event)
        last_update_id = event["u"]


def apply_event(event):
    for bid in event["b"]:
        price = bid[0]
        quantity = bid[1]
        if float(quantity) == 0:
            if price in order_book["bids"]:
                del order_book["bids"][price]
        else:
            order_book["bids"][price] = quantity

    for ask in event["a"]:
        price = ask[0]
        quantity = ask[1]
        if float(quantity) == 0:
            if price in order_book["asks"]:
                del order_book["asks"][price]
        else:
            order_book["asks"][price] = quantity


def init_order_book(pair):
    depth_snapshot = get_depth_snapshot(pair)

    for bid in depth_snapshot["bids"]:
        order_book["bids"][bid[0]] = bid[1]

    for ask in depth_snapshot["asks"]:
        order_book["asks"][ask[0]] = ask[1]

    time.sleep(3)
    sync_events(depth_snapshot)


pair = sys.argv[1]
thread = threading.Thread(target=connect_to_stream, args=[pair])
thread.start()
init_order_book(pair)
