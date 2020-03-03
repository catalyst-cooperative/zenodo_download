#!/usr/bin/env python
# -*- coding: utf -*-

import zen_dl
import random


class TestZenDl:
    """Validate the zen_dl library as needed for TDD"""

    def test_doi_to_url(self):
        """Ensure we get the right ID from a zenodo DOI"""

        zd = zen_dl.ZenDownloader("local testing", testing=True)

        doi_id = random.randint(100000, 999999)
        doi = "10.%d/zenodo.%d" % (random.randint(1000,9999), doi_id)

        assert(zd.doi_to_url(doi) == "%s/deposit/depositions/%d" % (
            zd.api_root, doi_id))
