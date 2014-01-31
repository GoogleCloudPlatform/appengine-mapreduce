#!/usr/bin/env python
"""Map job package."""

# All the public API should be imported here.
# 1. Seasoned Python user should simply import this package.
# 2. Other users may import individual files so filenames should still have
#    "map_job" prefix. But adding the prefix won't mandate the first type
#    of user to type more.
# 3. Class names should not have "map_job" prefix.
from .map_job_config import JobConfig
from .map_job_control import start
from .mapper import Mapper
