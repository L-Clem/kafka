from producer import *
from consumer import *

def main():
    currency = "bitcoin"
    #rank = currency_rank_poller(currency)
    for x in range(50):
        rate = currency_rate_poller(currency)
        send("currency_rate_evolution", currency, "{\"key\": \"" + currency + "\", \"val\": " + rate + "}")
    #consume(["currency_mean_volume_month"])

if __name__ == "__main__":
    main()