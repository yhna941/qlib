# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# pylint: skip-file
# flake8: noqa

import pathlib
import pickle
import pandas as pd
from ruamel.yaml import YAML
from ...data import D
from ...config import C
from ...log import get_module_logger
from ...utils import get_next_trading_date
from ...backtest.exchange import Exchange

log = get_module_logger("utils")


def load_instance(file_path):
    """
    load a pickle file
        Parameter
           file_path : string / pathlib.Path()
                path of file to be loaded
        :return
            An instance loaded from file
    """
    file_path = pathlib.Path(file_path)
    if not file_path.exists():
        raise ValueError("Cannot find file {}".format(file_path))
    with file_path.open("rb") as fr:
        instance = pickle.load(fr)
    return instance


def save_instance(instance, file_path):
    """
    save(dump) an instance to a pickle file
        Parameter
            instance :
                data to be dumped
            file_path : string / pathlib.Path()
                path of file to be dumped
    """
    file_path = pathlib.Path(file_path)
    with file_path.open("wb") as fr:
        pickle.dump(instance, fr, C.dump_protocol_version)


def create_user_folder(path):
    path = pathlib.Path(path)
    if path.exists():
        return
    path.mkdir(parents=True)
    head = pd.DataFrame(columns=("user_id", "add_date"))
    head.to_csv(path / "users.csv", index=None)


def prepare(um, today, user_id, exchange_config=None):
    """
    1. Get the dates that need to do trading till today for user {user_id}
        dates[0] indicate the latest trading date of User{user_id},
        if User{user_id} haven't do trading before, than dates[0] presents the init date of User{user_id}.
    2. Set the exchange with exchange_config file

        Parameter
            um : UserManager()
            today : pd.Timestamp()
            user_id : str
        :return
            dates : list of pd.Timestamp
            trade_exchange : Exchange()
    """
    # get latest trading date for {user_id}
    # if is None, indicate it haven't traded, then last trading date is init date of {user_id}
    latest_trading_date = um.users[user_id].get_latest_trading_date()
    if not latest_trading_date:
        latest_trading_date = um.user_record.loc[user_id][0]

    if str(today.date()) < latest_trading_date:
        log.warning("user_id:{}, last trading date {} after today {}".format(user_id, latest_trading_date, today))
        return [pd.Timestamp(latest_trading_date)], None

    dates = D.calendar(
        start_time=pd.Timestamp(latest_trading_date),
        end_time=pd.Timestamp(today),
        future=True,
    )
    dates = list(dates)
    dates.append(get_next_trading_date(dates[-1], future=True))
    if exchange_config:
        with pathlib.Path(exchange_config).open("r") as fp:
            yaml = YAML(typ="safe", pure=True)
            exchange_paras = yaml.load(fp)
    else:
        exchange_paras = {}
    trade_exchange = Exchange(trade_dates=dates, **exchange_paras)
    return dates, trade_exchange


def save_score_series(score_series, user_path, trade_date):
    """Save the score_series into a .csv file.
    The columns of saved file is
        [stock_id, score]

    Parameter
    ---------
        order_list: [Order()]
            list of Order()
        date: pd.Timestamp
            the date to save the order list
        user_path: str / pathlib.Path()
            the sub folder to save user data
    """
    user_path = pathlib.Path(user_path)
    YYYY, MM, DD = str(trade_date.date()).split("-")
    folder_path = user_path / "score" / YYYY / MM
    if not folder_path.exists():
        folder_path.mkdir(parents=True)
    file_path = folder_path / "score_{}.csv".format(str(trade_date.date()))
    score_series.to_csv(file_path)


def load_score_series(user_path, trade_date):
    """Save the score_series into a .csv file.
    The columns of saved file is
        [stock_id, score]

    Parameter
    ---------
        order_list: [Order()]
            list of Order()
        date: pd.Timestamp
            the date to save the order list
        user_path: str / pathlib.Path()
            the sub folder to save user data
    """
    user_path = pathlib.Path(user_path)
    YYYY, MM, DD = str(trade_date.date()).split("-")
    folder_path = user_path / "score" / YYYY / MM
    if not folder_path.exists():
        folder_path.mkdir(parents=True)
    file_path = folder_path / "score_{}.csv".format(str(trade_date.date()))
    score_series = pd.read_csv(file_path, index_col=0, header=None, names=["instrument", "score"])
    return score_series


def save_order_list(order_list, user_path, trade_date):
    """
    Save the order list into a json file.
    Will calculate the real amount in order according to factors at date.

    The format in json file like
    {"sell": {"stock_id": amount, ...}
    ,"buy": {"stock_id": amount, ...}}

        :param
            order_list: [Order()]
                list of Order()
            date: pd.Timestamp
                the date to save the order list
            user_path: str / pathlib.Path()
                the sub folder to save user data
    """
    user_path = pathlib.Path(user_path)
    YYYY, MM, DD = str(trade_date.date()).split("-")
    folder_path = user_path / "trade" / YYYY / MM
    if not folder_path.exists():
        folder_path.mkdir(parents=True)
    sell = {}
    buy = {}
    for order in order_list:
        if order.direction == 0:  # sell
            sell[order.stock_id] = [order.amount, order.factor]
        else:
            buy[order.stock_id] = [order.amount, order.factor]
    order_dict = {"sell": sell, "buy": buy}
    file_path = folder_path / "orderlist_{}.json".format(str(trade_date.date()))
    with file_path.open("w") as fp:
        json.dump(order_dict, fp)


def load_order_list(user_path, trade_date):
    user_path = pathlib.Path(user_path)
    YYYY, MM, DD = str(trade_date.date()).split("-")
    path = user_path / "trade" / YYYY / MM / "orderlist_{}.json".format(str(trade_date.date()))
    if not path.exists():
        raise ValueError("File {} not exists!".format(path))
    # get orders
    with path.open("r") as fp:
        order_dict = json.load(fp)
    order_list = []
    for stock_id in order_dict["sell"]:
        amount, factor = order_dict["sell"][stock_id]
        order = Order(
            stock_id=stock_id,
            amount=amount,
            trade_date=pd.Timestamp(trade_date),
            direction=Order.SELL,
            factor=factor,
        )
        order_list.append(order)
    for stock_id in order_dict["buy"]:
        amount, factor = order_dict["buy"][stock_id]
        order = Order(
            stock_id=stock_id,
            amount=amount,
            trade_date=pd.Timestamp(trade_date),
            direction=Order.BUY,
            factor=factor,
        )
        order_list.append(order)
    return order_list