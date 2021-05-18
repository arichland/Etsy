_author_ = 'arichland'
import listings
import orders


def get_orders():
    ord = orders.Main()
    ord.new_receipts()
    #ord.update_receipts()
    #ord.new_transactions()
#get_orders()

def get_listings():
    lst = listings.Main()
    #lst.listings()
    lst.products()
get_listings()
