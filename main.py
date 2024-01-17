from producer import *
from consumer import *

def main():
    currency = "bitcoin"
    rank = currency_rank_poller(currency)
    send("currency_rank", currency, rank)
    consume(["currency_rank"])

if __name__ == "__main__":
    main()