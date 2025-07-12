from tinkoff.invest.utils import quotation_to_decimal

async def get_figi_by_ticker(ticker: str, client):
    methods = ["shares", "bonds", "etfs", "currencies", "futures"]
    for method in methods:
        response = await getattr(client.instruments, method)()
        for item in response.instruments:
            if item.ticker == ticker:
                return {
                    "figi": item.figi,
                    "uid": item.uid,
                    "name": item.name,
                    "class_code": item.class_code,
                    "type": method,
                    "min_price_increment": quotation_to_decimal(item.min_price_increment),
                    "lot": item.lot,
                    "currency": item.currency,
                    "exchange": item.exchange,
                }
    return None