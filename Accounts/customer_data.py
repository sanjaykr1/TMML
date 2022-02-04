import logging

from Accounts.utility import Utility

logger = logging.getLogger(__name__)


class CustomerData(Utility):
    """
    CustomerData class to read customer data.
    """
    def __init__(self, filename):
        super().__init__()
        logger.info("Creating customer data class object.")
        self.filename = filename
        self.df = super().readfile(filename)

