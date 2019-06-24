# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The REANA client class is a wrapper around the relevant parts of the
reana-client implementation. The client provides the functionality that is
required by the workflow engine to execute workflows, cancel workflow
execution, get workflow status, and download workflow result files.
"""

import os

from benchreana.error import REANABackendError

import reana_client.api.client as reana


"""Definition of possible workflow states. Uses lists since there is a 1:n
mapping between the states of workflow benchmarks and REANA workflow states.
"""
REANA_STATE_PENDING = ['created', 'queued']
REANA_STATE_RUNNING = ['running']
REANA_STATE_ERROR = ['failed', 'stopped', 'deleted']
REANA_STATE_SUCCESS = ['finished']


class REANAClient(object):
    """The REANA client class is a wrapper around the relevant parts of the
    reana-client implementation. The client provides the functionality that is
    required by the workflow engine to execute workflows, cancel workflow
    execution, get workflow status, and download workflow result files.

    The main reason for having a separate wrapper around the required methods
    from the reana-client implementation is for testing purposes. Having the
    REANA client allows to test the functionality of the REANA workflow engine
    backend without having access to a running REANA cluster.
    """
    def __init__(self, name=None, access_token=None):
        """Initialize the REANA client. The client requires the REANA access
        token for the user. If the token is not given as an argument the default
        environment variable REANA_ACCESS_TOKEN is expected to contain the
        token. If this is not the case an error is raised.

        In REANA each workflow has a unique name. The benchmarks currently do
        not make use of this name. By default, all workflows that are created
        by the client at a REANA cluster have the same name (prefix).

        Parameters
        ----------
        name: string, optional
            Optional name prefix for all created workflows
        access_token: string, optional
            Access token for the REANA cluster

        Raises
        ------
        RuntimeError
        """
        self.name = name if not name is None else 'workflow'
        if not access_token is None:
            self.access_token = access_token
        else:
             self.access_token = os.getenv('REANA_ACCESS_TOKEN', None)
        if self.access_token is None:
            raise RuntimeError('REANA access token not defined defined')

    def create_workflow(self, reana_specification):
        """Create a new instance of a workflow from the given workflow
        specification.

        Parameters
        ----------
        reana_specification: dict
            REANA workflow specification

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

        Raises
        ------
        benchreana.error.REANABackendError
        """
        try:
            return reana.create_workflow(
                reana_specification,
                self.name,
                self.access_token
            )
        except Exception as ex:
            raise REANABackendError(message=str(ex))

    def download_file(self, workflow_id, source, target):
        """Download file from relative location at REANA cluster to target path.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        source: string
            Relative path to source file in workflow workspace at REANA cluster
        target: string
            Path to target file on local disk

        Raises
        ------
        benchreana.error.REANABackendError
        """
        try:
            raw_bytes = reana.download_file(
                workflow_id,
                source,
                self.access_token
            )
            if not os.path.exists(os.path.dirname(target)):
                os.makedirs(os.path.dirname(target))
            with open(target, 'wb') as f:
                f.write(raw_bytes)
        except Exception as ex:
            raise REANABackendError(message=str(ex))

    def get_current_status(self, workflow_id):
        """Get information about the current state of a given workflow.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

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

        Raises
        ------
        benchreana.error.REANABackendError
        """
        try:
            return reana.get_workflow_status(workflow_id, self.access_token)
        except Exception as ex:
            raise REANABackendError(message=str(ex))

    def start_workflow(self, workflow_id):
        """Start the execution of the workflow with the given identifier.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

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

        Raises
        ------
        benchreana.error.REANABackendError
        """
        try:
            return reana.start_workflow(workflow_id, self.access_token, dict())
        except Exception as ex:
            raise REANABackendError(message=str(ex))

    def stop_workflow(self, workflow_id):
        """Stop the execution of the workflow with the given identifier.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

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

        Raises
        ------
        benchreana.error.REANABackendError
        """
        try:
            return reana.stop_workflow(workflow_id, True, self.access_token)
        except Exception as ex:
            raise REANABackendError(message=str(ex))

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
