# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Fake REANA API for test purposes."""

import os

import flowserv.core.util as util
import flowservreana.client as rn


class REANATestAPI(object):
    """Implementation of all REANA API methods that are used by the REANAClient
    implementation. Used to simulate the execution of a workflow at the REANA
    backend. Expects a workflow template with a single file inputs/to-do.json.
    The file contains an object with an 'action' element. If the element
    value is 'ERROR' workflow execution results in an error state. Otherwise,
    the content of the file is written to results/outputs.json. Workflow
    execution starts with the second call the get_workflow_state.
    """
    def __init__(self, basedir):
        """Initialize the base directory for workflow runs.

        Parameters
        ----------
        basedir: string
            Path to base directory for workflow runs
        """
        self.basedir = basedir
        self.status = None

    def create_workflow(self, workflow_spec, name, token):
        """Simulate creation of a workflow at the backend. Generates a unique
        workflow identifier and a directory for workflow files. Ignores the
        name and token arguments.

        Parameters
        ----------
        workflow_spec: dict
            REANA workflow specification.
        name: string
            Workflow name prefix.
        token: string
            REANA access token.

        Returns
        -------
        flowserv.controller.remote.wrokflow.RemoteWorkflowHandle
        """
        workflow_id = util.get_unique_identifier()
        util.create_dir(os.path.join(self.basedir, workflow_id))
        self.status = rn.REANA_STATE_PENDING[0]
        return {'workflow_id': workflow_id}

    def download_file(self, workflow_id, file_name, token):
        """Download the requested file from the workflow workspace.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier.
        file_name: string
            Relative path in the workflow workspace to the file requested.
        token: string
            REANA access token.
        """
        workflowdir = os.path.join(self.basedir, workflow_id)
        with open(os.path.join(workflowdir, file_name), 'rb') as f:
            return f.read()

    def get_workflow_status(self, workflow, token):
        """Get status of previously created workflow.

        Parameters
        ----------
        workflow: string
            Name or id of previously created workflow.
        token: string
            REANA access token.
        """
        status = self.status
        if status in rn.REANA_STATE_PENDING:
            self.status = rn.REANA_STATE_RUNNING[0]
        elif status in rn.REANA_STATE_RUNNING:
            workflowdir = os.path.join(self.basedir, workflow)
            infile = os.path.join(workflowdir, 'inputs/to-do.json')
            doc = util.read_object(filename=infile)
            if doc['action'] == 'ERROR':
                self.status = rn.REANA_STATE_ERROR[0]
            else:
                self.status = rn.REANA_STATE_SUCCESS[0]
                util.create_dir(os.path.join(workflowdir, 'results'))
                outfile = os.path.join(workflowdir, 'results/outputs.json')
                util.write_object(obj=doc, filename=outfile)
        return {'status': self.status}

    def start_workflow(self, workflow, token, parameters):
        """Simulate starting a workflow. Has no effect. Workflows are started
        when the get_workflow_status method is called (for test purposes).

        Parameters
        ----------
        workflow: string
            Name or id of previously created workflow.
        token: string
            REANA access token.
        parameters: dict
            Workflow parameters to override the original ones (after workflow
            creation).
        """
        pass

    def stop_workflow(self, workflow_id, force_stop, token):
        """Simulate stopping the workflow. Does nothing at this point.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier.
        force_stop: bool
            Value of the force_stop parameter for API request.
        token: string
            REANA access token.
        """
        pass

    def upload_file(self, workflow_id, file, filename, token):
        """Upload file to workflow workspace.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier.
        file: FileObject
            Content of the file that is being uploaded.
        filename: string
            Relative path of the target file in the workflow workspace.
        token: string
            REANA access token.
        """
        workflowdir = os.path.join(self.basedir, workflow_id)
        util.create_directories(workflowdir, [filename])
        with open(os.path.join(workflowdir, filename), 'wb') as f:
            f.write(file.read())
