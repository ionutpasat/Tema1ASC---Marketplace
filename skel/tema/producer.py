"""
This module represents the Producer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

from threading import Thread
from time import sleep

class Producer(Thread):
    """
    Class that represents a producer.
    """

    def __init__(self, products, marketplace, republish_wait_time, **kwargs):
        """
        Constructor.

        @type products: List()
        @param products: a list of products that the producer will produce

        @type marketplace: Marketplace
        @param marketplace: a reference to the marketplace

        @type republish_wait_time: Time
        @param republish_wait_time: the number of seconds that a producer must
        wait until the marketplace becomes available

        @type kwargs:
        @param kwargs: other arguments that are passed to the Thread's __init__()
        """
        super().__init__(**kwargs)
        self.products = products
        self.marketplace = marketplace
        self.republish_wait_time = republish_wait_time
        self.id_no = 0

    def run(self):
        self.id_no = self.marketplace.register_producer()
        while True:
            for product in self.products:
                quantity = int(product[1])
                while quantity > 0:
                    response = False
                    while response is False:
                        response = self.marketplace.publish(self.id_no, product[0])
                        if response is False:
                            sleep(self.republish_wait_time)
                    sleep(product[2])
                    quantity -= 1
