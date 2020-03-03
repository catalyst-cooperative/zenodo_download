#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import re
import requests
import yaml


class ZenDownloader:
    """
    Handle connections and downloading of Zenodo Source archives
    """

    def __init__(self, token, loglevel="WARNING", verbose=False, testing=False):
        """
        Create a ZenDownloader instance

        Args:
            token: str, the zenodo API access token
            loglevel: str, logging level
            verbose: boolean. If true, logs printed to stdout
            testing: boolean. If true, use the sandbox server instead of
                     production

        Returns:
            ZenDownloader instance
        """
        self.token = token

        logger = logging.Logger(__name__)
        logger.setLevel(loglevel)

        if verbose:
            logger.addHandler(logging.StreamHandler())

        self.logger = logger

        with open(os.path.expanduser("~/.pudl.yaml"), "r") as f:
            cfg = yaml.load(f.read(), Loader=yaml.FullLoader)

            if testing:
                self.api_root = "https://sandbox.zenodo.org/api"
                self.dois = cfg["zenodo_download"]["sandbox_dois"]
            else:
                self.api_root = "https://zenodo.org/api"
                self.dois = cfg["zenodo_downloads"]["production_dois"]

        self.output_root = os.environ.get("PUDL_IN", cfg["pudl_in"])

    def retrieve(self, archive, filters=None):
        """
        Download the files from the provided archive to the appropriate
        PUDL_IN directory, coordinating other methods as needed

        Args:
            archive (str): name of the archive, must be available in the
                           ~/.pudl.yaml
            filters (dict): limit retrieved files to those where the
                            datapackage.json["parts"] key & val pairs match
                            those in the filter
        Returns:
            None
        """
        if filters is None:
            filters = {}

        datapackage = self.datapackage_contents(archive)

        output_dir = os.path.join(self.output_root, archive)
        os.makedirs(output_dir, exist_ok=True)

        for resource in datapackage["resources"]:

            if self.passes_filters(resource, filters):
                local_path = self.download_resource(resource, output_dir)
                yield local_path

    def datapackage_contents(self, archive):
        """
        Produce the contents of the remote datapackage.json file

        Args:
            archive (str): name of the archive, must be available in the

        Return:
            dict representation of the datapackage.json file, as contained
                within the remote archive
        """
        try:
            doi = self.dois[archive]
        except KeyError:
            msg = "No DOI for %s in ~/.pudl.yaml" % archive
            self.logger.error(msg)
            raise ValueError(msg)

        try:
            dpkg_url = self.doi_to_url(doi)
        except KeyError:
            msg = "No datapackage for %s" % doi
            self.logger.error(msg)
            raise ValueError(msg)

        response = requests.get(dpkg_url, params={"access_token": self.token})
        jsr = response.json()

        if response.status_code > 299:
            msg = "Failed to retrieve %s" % dpkg_url
            self.logger.error(msg)
            raise ValueError(msg)

        files = {x["filename"]: x for x in jsr["files"]}

        response = requests.get(
            files["datapackage.json"]["links"]["download"],
            params={"access_token": self.token})

        if response.status_code > 299:
            msg = "Failed to retrieve datapackage for %s: %s" % (
                archive, response.text)
            self.logger.error(msg)
            raise ValueError(msg)

        return yaml.load(response.text, Loader=yaml.FullLoader)

    def doi_to_url(self, doi):
        """
        Given a DOI, produce the API url to retrieve it

        Args:
            doi: str, the doi (concept doi) to retrieve, per
                 https://help.zenodo.org/

        Returns:
            url to get the deposition from the api
        """
        match = re.search(r"zenodo.([\d]+)", doi)

        if match is None:
            msg = "Invalid doi %s" % doi
            self.logger.error(msg)
            raise ValueError(msg)

        zen_id = int(match.groups()[0])
        return "%s/deposit/depositions/%d" % (self.api_root, zen_id)

    def passes_filters(self, resource, filters):
        """
        Test whether file metadata passes given filters

        Args:
            resource: dict, a "resource" descriptior from a frictionless
                      datapackage
            filters: dict, pairs that must match in order for a
                     resource to pass
        Returns:
            boolean, True if the resource parts pass the filters
        """
        for key, _ in filters.items():

            part_val = resource["parts"].get(key, None)

            if part_val != filters[key]:
                self.logger.debug(
                    "Filtered %s on %s: %s != %s", resource["name"], key,
                    part_val, filters[key])
                return False

        return True

    def download_resource(self, resource, output_dir):
        """
        Download a frictionless datapackage resource

        Args:
            resource: dict, a "resource" descriptior from a frictionless
                      datapackage
            output_dir: str, the output directory, must already exist

        Returns:
            str, path to the locally saved resource, or None on failure
        """
        local_path = os.path.join(output_dir, resource["name"])
        response = requests.get(
            resource["path"], params={"access_token": self.token})

        if response.status_code >= 299:
            self.logger.warning(
                "Failed to download %s, %s", resource["path"], response.text)
            return

        with open(local_path, "wb") as f:
            f.write(response.content)

        return local_path
