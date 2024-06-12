import yaml
import time
import requests
import pandas as pd

last_message_time = 0
cooldown_period = 60

with open("config.yaml", "r") as stream:
    config = yaml.safe_load(stream)


def measure_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        file = open("execution_log.txt", "a")
        file.write(f"Execution time of {func.__name__}: {execution_time:.4f} seconds\n")
        file.close()
        return result

    return wrapper


def generate_message(deviation_type, pair, deviaton, mid_price, price_above, price_below, volume_above, volume_below, avg_volume_above, avg_volume_below):
    return (
        f"🌐 Anomaly volume {deviation_type} ({(deviaton * 100):.2f}%) for {pair.upper()}\n"
        f"├ Price: {mid_price:.2f}\n"
        f"├ Price (+{config['monitoring']['distance'] * 100}%): {price_above:.2f}\n"
        f"├ Price (-{config['monitoring']['distance'] * 100}%): {price_below:.2f}\n"
        f"├ Volume Above: {volume_above:.2f}\n├ Volume Below: {volume_below:.2f}\n"
        f"├ Average Volume Above ({config['monitoring']['period']} sec.): {avg_volume_above:.2f}\n"
        f"└ Average Volume Below ({config['monitoring']['period']} sec.): {avg_volume_below:.2f}"
    )


def send_telegram_message(message):
    global last_message_time

    if time.time() - last_message_time < cooldown_period:
        return

    last_message_time = time.time()
    payload = {"chat_id": config["telegram"]["chat_id"], "text": message}
    requests.post(
        f"https://api.telegram.org/bot{config['telegram']['bot_token']}/sendMessage",
        json=payload,
    )


@measure_execution_time
def calculate_prices_and_volumes(order_book):
    if not order_book["bids"] or not order_book["asks"]:
        return None, None, None, None, None, None, None

    bids_df = pd.DataFrame(
        list(order_book["bids"].items()), columns=["Price", "Quantity"]
    )
    asks_df = pd.DataFrame(
        list(order_book["asks"].items()), columns=["Price", "Quantity"]
    )

    best_bid = bids_df["Price"].astype(float).max()
    best_ask = asks_df["Price"].astype(float).min()

    mid_price = (best_bid + best_ask) / 2

    price_above = mid_price * (1 + config["monitoring"]["distance"])
    price_below = mid_price * (1 - config["monitoring"]["distance"])

    volume_above = (
        asks_df[asks_df["Price"].astype(float) <= price_above]["Quantity"]
        .astype(float)
        .sum()
    )
    volume_below = (
        bids_df[bids_df["Price"].astype(float) >= price_below]["Quantity"]
        .astype(float)
        .sum()
    )

    return (
        mid_price,
        price_above,
        price_below,
        volume_above,
        volume_below,
    )
