# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""File uploader for REANA cluster. The file loader is used by the generic
file upload function.
"""

from benchreana.client import REANAClient

import reana_client.api.client as reana


class FileUploader(object):
    """File upload function that uploads files for a given workflow to a REANA
    cluster. An instance of the class can only be used to upload files for a
    specific workflow.
    """
    def __init__(self, workflow_id, client):
        """Initialize the workflow identifier and the REANA client that is used
        for file upload.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier that was returned by the REANA cluster
            when the workflow was created.
        client: benchreana.client.REANAClient, optional
            Client to interact with the REANA cluster
        """
        self.workflow_id = workflow_id
        # Set the REANA client. If the client is not given as an argument the
        # default client implementation is used.
        if client is None:
            self.reana = REANAClient()
        else:
            self.reana = client

    def __call__(self, source, target):
        """Upload the local source file to the relative target location in the
        workflow workspace on the REANA cluster.

        Parameters
        ----------
        source: string
            Path to file on disk
        target: string
            Relative target path for file in workflow workspace

        Raises
        ------
        benchreana.error.REANABackendError
        """
        self.reana.upload_file(self.workflow_id, source, target)
