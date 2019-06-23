# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""
"""

from benchreana.error import REANABackendError

import reana_client.api.client as reana


"""Definition of possible workflow states."""
REANA_STATE_PENDING = 'created'
REANA_STATE_RUNNING = 'running'
REANA_STATE_ERROR = 'failed'
REANA_STATE_SUCCESS = 'finished'


class REANAClient(object):
    """
    """
    def __init__(self, server_url=None, access_token=None):
        """
        """
        pass

    def create_workflow(self, reana_specification):
        """

        Returns
        -------
        dict

        "schema": {
          "properties": {
            "message": {
              "type": "string"
            },
            "workflow_id": {
              "type": "string"
            },
            "workflow_name": {
              "type": "string"
            }
          },
          "type": "object"
        }

        """
        pass

    def download_file(self, source, target):
        """
        """
        pass

    def get_current_status(self, workflow_id):
        """

        Returns
        -------
        dict

        "schema": {
          "properties": {
            "created": {
              "type": "string"
            },
            "id": {
              "type": "string"
            },
            "logs": {
              "type": "string"
            },
            "name": {
              "type": "string"
            },
            "progress": {
              "type": "object"
            },
            "status": {
              "type": "string"
            },
            "user": {
              "type": "string"
            }
          },
          "type": "object"
        }
        """
        pass

    def start_workflow(self, workflow_id):
        """

        Returns
        -------
        dict

        "schema": {
          "properties": {
            "message": {
              "type": "string"
            },
            "status": {
              "type": "string"
            },
            "user": {
              "type": "string"
            },
            "workflow_id": {
              "type": "string"
            },
            "workflow_name": {
              "type": "string"
            }
          },
          "type": "object"
        }

        """
        pass

    def upload_file(self, workflow_id, source, target):
        """Upload a local source file to the target location in the workflow
        workspace on the REANA cluster. This is a wrapper around the respective
        upload file method of the REANA API client.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier that was returned by the REANA cluster
            when the workflow was created.
        source: string
            Path to file on disk
        target: string
            Relative target path for file in workflow workspace

        Raises
        ------
        benchreana.error.REANABackendError
        """
        # If the source references a directory the whole directory tree is
        # copied
        if os.path.isdir(source):
            for root, dirs, files in os.walk(source, topdown=False):
                for next_path in files + dirs:
                    self.upload_file(
                        workflow_id,
                        os.path.join(source, next_path),
                        os.path.join(target, next_path)
                    )
        else:
            # The REANA client file upload function expects a file object for
            # the file that is being uploaded.
            with open(soure, 'rb') as f:
                try:
                    reana.upload_file(
                        workflow_id,
                        f,
                        target,
                        self.access_token
                    )
                except Exception as ex:
                    raise REANABackendError(str(ex))
