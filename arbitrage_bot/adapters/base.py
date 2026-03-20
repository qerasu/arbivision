import abc


class BaseAdapter(abc.ABC):
    @abc.abstractmethod
    async def fetch_markets(self):
        pass


    @abc.abstractmethod
    async def fetch_orderbook(self, market_id):
        pass