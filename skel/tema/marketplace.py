"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
import unittest
import logging
import logging.handlers
from threading import Lock

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ch = logging.handlers.RotatingFileHandler("marketplace.log", 'a', 50000, 5)
ch.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

logger.addHandler(ch)


class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """

    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.queue_size_per_producer = queue_size_per_producer
        self.carts = []
        self.producers = []
        self.returned_products = []
        self.placed_orders = 0
        self.carts_lock = Lock()
        self.producers_lock = Lock()

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        with self.producers_lock:
            producer_id = len(self.producers)
            self.producers.append({'id': producer_id, 'queue': []})
            logger.info("Producer with id %s started!", producer_id)
            return producer_id

    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """
        with self.producers_lock:
            producer = self.producers[producer_id]
            if len(producer['queue']) < self.queue_size_per_producer:
                producer['queue'].append(product)
                logger.info("Producer %s published %s", producer_id, product)
                return True
        logger.info("Producer %s did not manage to publish %s", producer_id, product)
        return False

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        with self.carts_lock:
            cart_id = len(self.carts)
            self.carts.append([])
            logger.info("Cart with id %s was created!", cart_id)
            return cart_id

    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """
        cart = self.carts[cart_id]
        if product in self.returned_products:
            cart.append(product)
            self.returned_products.remove(product)
            logger.info("%s was added to cart %s", product, cart_id)
            return True
        for producer in self.producers:
            if product in producer['queue']:
                cart.append(product)
                producer['queue'].remove(product)
                logger.info("%s was added to cart %s", product, cart_id)
                return True
        logger.info("%s was not added to cart %s", product, cart_id)
        return False

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        self.returned_products.append(product)
        self.carts[cart_id].remove(product)
        logger.info("%s was removed from cart %s", product, cart_id)

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        with self.carts_lock:
            self.placed_orders += 1
        return self.carts[cart_id]