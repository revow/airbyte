#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from typing import TYPE_CHECKING, Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource

from .client import DatabricksClient
from .streams import DatabricksStream

if TYPE_CHECKING:
    from airbyte_cdk.sources.streams import Stream


class SourceDatabricks(AbstractSource):
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        """
        Test the connection to Databricks using the provided configuration.
        
        :param config: the user-input config object conforming to the connector's spec.json
        :param logger: logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            client = DatabricksClient(config)
            return client.check_connection()
        except Exception as e:
            logger.error(f"Connection check failed: {str(e)}")
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List["Stream"]:
        """
        Create streams based on the configuration.
        
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        :return: A list of streams to sync.
        """
        return [DatabricksStream(config)]
