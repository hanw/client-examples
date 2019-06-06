import os
import json
import datetime
import asyncio
import uvloop
import logging
from polygon_streamer import PolygonStreamer

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class StocksStreamer(PolygonStreamer):
    exchange = {0: "-",
                1: "AMX",
                2: "NSD",
                3: "XCIS",
                4: "FINRA",
                5: "CQS",
                6: "XISX",
                7: "EDGA",
                8: "EDGX",
                9: "XCHI",
                10: "XNYS",
                11: "ARCX",
                12: "XNGS",
                13: "CTS",
                14: "OOTC",
                141: "XOTC",
                142: "PSGM",
                143: "PINX",
                144: "OTCB",
                145: "OTCQ",
                15: "IEXG",
                16: "XCBO",
                17: "PHLX",
                18: "BATY",
                19: "BATS",
                33: "XBOS"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def callback(self, message_str):
        try:
            def mapper(message):
                if message["ev"] == "T":
                    exc = self.exchange[message["x"]]
                    return "%s %-6s %-6s %-2s %s" %(message["sym"], message["p"], message["s"], exc, message["c"])
                else:
                    return message

            for message in json.loads(message_str):
                print(mapper(message))
            return

        except Exception as e:
            logging.error("{}".format(e.args))


def main():
    polygon_api_key = os.environ.get("polygon_api_key", "")
    cluster = "/stocks"
    symbols_str = "T.CHK"
    streamer = StocksStreamer(api_key=polygon_api_key, cluster=cluster, symbols_str=symbols_str)
    streamer.start()

if __name__ == '__main__':
    main()
