#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_roman_coin_api import SourceRomanCoinApi

if __name__ == "__main__":
    source = SourceRomanCoinApi()
    launch(source, sys.argv[1:])
