import logging

from Accounts.utility import Utility

logger = logging.getLogger(__name__)


class AccountData(Utility):
    """
    Account Data class for reading account data
    """
    def __init__(self, filename):
        super().__init__()
        logger.info("Creating Account data class object")
        self.filename = filename
        self.df = super().readfile(self.filename)
