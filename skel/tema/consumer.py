"""
This module represents the Consumer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

from threading import Thread
from time import sleep

class Consumer(Thread):
    """
    Class that represents a consumer.
    """

    def __init__(self, carts, marketplace, retry_wait_time, **kwargs):
        """
        Constructor.

        :type carts: List
        :param carts: a list of add and remove operations

        :type marketplace: Marketplace
        :param marketplace: a reference to the marketplace

        :type retry_wait_time: Time
        :param retry_wait_time: the number of seconds that a producer must wait
        until the Marketplace becomes available

        :type kwargs:
        :param kwargs: other arguments that are passed to the Thread's __init__()
        """
        super().__init__(**kwargs)
        self.carts = carts
        self.marketplace = marketplace
        self.id_no = 0
        self.retry_wait_time = retry_wait_time
        self.kwargs = kwargs
    def run(self):
        for batch in self.carts:
            self.id_no = self.marketplace.new_cart()
            for product in batch:
                quantity = product['quantity']
                while quantity > 0:
                    if product['type'] == "remove":
                        self.marketplace.remove_from_cart(
                            self.id_no, product['product'])
                    elif product['type'] == "add":
                        response = False
                        while response is False:
                            response = self.marketplace.add_to_cart(
                                self.id_no, product['product'])
                            sleep(self.retry_wait_time)
                    quantity -= 1
            receipt = self.marketplace.place_order(self.id_no)
            with self.marketplace.print_lock:
                for product in receipt:
                    print(f"{self.kwargs['name']} bought {product}")
