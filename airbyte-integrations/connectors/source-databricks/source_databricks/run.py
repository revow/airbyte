#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import sys

from airbyte_cdk.entrypoint import launch

from .source import SourceDatabricks


def run():
    """Launch the Databricks source connector."""
    source = SourceDatabricks()
    launch(source, sys.argv[1:])
