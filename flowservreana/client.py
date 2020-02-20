# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The REANA client class is a wrapper around the relevant parts of the
reana-client implementation. The client provides the functionality that is
required by the workflow engine to execute workflows, cancel workflow
execution, get workflow status, and download workflow result files.
"""

import os

from flowserv.controller.remote.client import RemoteClient
from flowserv.controller.remote.workflow import RemoteWorkflowHandle
from flowservreana.workflow import REANAWorkflow


"""Definition of possible workflow states. Uses lists since there is a 1:n
mapping between the states of workflow benchmarks and REANA workflow states.
"""
REANA_STATE_PENDING = ['created', 'queued']
REANA_STATE_RUNNING = ['running']
REANA_STATE_ERROR = ['failed', 'stopped', 'deleted']
REANA_STATE_SUCCESS = ['finished']

REANA_ACTIVE_STATE = REANA_STATE_PENDING + REANA_STATE_RUNNING


class REANAClient(RemoteClient):
    """The REANA client class is a wrapper around the relevant parts of the
    reana-client implementation. The client provides the functionality that is
    required by the workflow engine to execute workflows, cancel workflow
    execution, get workflow status, and download workflow result files.
    """
    def __init__(self, name=None, access_token=None, reana_client=None):
        """Initialize the REANA client. The client requires the REANA access
        token for the user. If the token is not given as an argument the
        default environment variable REANA_ACCESS_TOKEN is expected to contain
        the token. If this is not the case, an error is raised.

        In REANA each workflow has a unique name. The benchmarks currently do
        not make use of this name. By default, all workflows that are created
        by the client at a REANA cluster have the same name (prefix).

        Parameters
        ----------
        name: string, optional
            Optional name prefix for all created workflows
        access_token: string, optional
            Access token for the REANA cluster
        reana_client: object, optional
            Client for the REANA Server API.

        Raises
        ------
        RuntimeError
        """
        # Default name for all created workflows
        self.name = name if name is not None else 'flowserv'
        # Set the access token. Raises error if no token is found
        if access_token is not None:
            self.token = access_token
        else:
            self.token = os.getenv('REANA_ACCESS_TOKEN', None)
        if self.token is None:
            raise RuntimeError('REANA access token not defined')
        # Initialize the reana client. If not client is given the api.client
        # module is used as default
        if reana_client is None:
            import reana_client.api.client as client
            self.reana = client
        else:
            self.reana = reana_client

    def create_workflow(self, run, template, arguments):
        """Create a new instance of a workflow from the given workflow
        template and user-provided arguments. After the workflow is created
        successfully, all required files are uploaded and the workflow is
        started at the REANA cluster.

        Parameters
        ----------
        run: flowserv.model.run.base.RunHandle
            Handle for the run that is being executed.
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations.
        arguments: dict(flowserv.model.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template.

        Returns
        -------
        flowserv.controller.remote.workflow.RemoteWorkflowHandle
        """
        # Create workflow instance for the REANA workflow specification at the
        # remote REANA cluster. Retrieves the workflow identifier for further
        # references.
        wf = REANAWorkflow(template, arguments)
        wf_spec = wf.workflow_spec
        r = self.reana.create_workflow(wf_spec, self.name, self.token)
        # Expected response schema:
        # "schema": {
        #     "properties": {
        #         "message": {
        #             "type": "string"
        #         },
        #         "workflow_id": {
        #             "type": "string"
        #         },
        #         "workflow_name": {
        #             "type": "string"
        #         }
        #     },
        #     "type": "object"
        # }
        workflow_id = r.get('workflow_id')
        state = modify_state(response=r, current_state=run.state)
        # Upload all required input files to the workspace of the created
        # workflow.
        for source, target in wf.upload_files:
            r = self.upload_file(workflow_id, source, target)
        # Start the workflow on the REANA cluster. Keep track of the workflow
        # status as reported by the REANA cluster.
        r = self.reana.start_workflow(workflow_id, self.token, dict())
        # Expected response schema:
        # "schema": {
        #     "properties": {
        #         "message": {
        #             "type": "string"
        #         },
        #         "status": {
        #             "type": "string"
        #         },
        #         "user": {
        #             "type": "string"
        #         },
        #         "workflow_id": {
        #             "type": "string"
        #         },
        #         "workflow_name": {
        #             "type": "string"
        #         }
        #     },
        #     "type": "object"
        # }
        return RemoteWorkflowHandle(
            identifier=workflow_id,
            state=state,
            output_files=wf.output_files
        )

    def download_file(self, workflow_id, source, target):
        """Download file from relative location at REANA cluster to target
        path.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        source: string
            Relative path to source file in workflow workspace at REANA cluster
        target: string
            Path to target file on local disk
        """
        raw_bytes = self.reana.download_file(workflow_id, source, self.token)
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        with open(target, 'wb') as f:
            f.write(raw_bytes)

    def get_workflow_state(self, workflow_id, current_state):
        """Get information about the current state of a given workflow.

        Note, if the returned result is SUCCESS the workflow resource files may
        not have been initialized properly. This will be done by the workflow
        controller. The timestamps, however, should be set accurately.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        current_state: flowserv.model.workflw.state.WorkflowState
            Last known state of the workflow by the workflow controller

        Returns
        -------
        flowserv.model.workflw.state.WorkflowState
        """
        r = self.reana.get_workflow_status(workflow_id, self.token)
        # Expected response schema:
        # "schema": {
        #     "properties": {
        #         "created": {
        #             "type": "string"
        #         },
        #         "id": {
        #             "type": "string"
        #         },
        #         "logs": {
        #             "type": "string"
        #         },
        #         "name": {
        #             "type": "string"
        #         },
        #         "progress": {
        #             "type": "object"
        #         },
        #         "status": {
        #             "type": "string"
        #         },
        #         "user": {
        #             "type": "string"
        #         }
        #     },
        #     "type": "object"
        # }
        return modify_state(response=r, current_state=current_state)

    def stop_workflow(self, workflow_id):
        """Stop the execution of the workflow with the given identifier.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        """
        self.reana.stop_workflow(workflow_id, True, self.token)

    def upload_file(self, workflow_id, source, target):
        """Upload a local source file to the target location in the workflow
        workspace on the REANA cluster. This is a wrapper around the respective
        upload file method of the REANA API client.

        The result is either the response dictionary for uploading a file or a
        list or responses for uploading files in a directory.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier that was returned by the REANA cluster
            when the workflow was created.
        source: string
            Path to file on disk
        target: string
            Relative target path for file in workflow workspace
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
            with open(source, 'rb') as f:
                self.reana.upload_file(workflow_id, f, target, self.token)


# -- Helper Methods -----------------------------------------------------------

def modify_state(response, current_state):
    """Modify the current workflow state based on the response from the REANA
    cluster. Expects that the response contains at least the 'status' element
    and an optional 'logs' element in case of an error.

    Parameters
    ----------
    response: dict
        Response object received from the REANA API.
    current_state: flowserv.model.workflw.state.WorkflowState
        Last known state of the workflow by the workflow controller

    Returns
    -------
    flowserv.model.workflow.state.WorkflowState
    """
    status = response.get('status')
    if status in REANA_STATE_RUNNING and current_state.is_pending():
        return current_state.start()
    elif status in REANA_STATE_ERROR and current_state.is_active():
        # The logs element containing error messages is optional in the API
        # response.
        msg = response.get('logs', 'unknown reason')
        return current_state.error(messages=[msg])
    elif status in REANA_STATE_SUCCESS and current_state.is_active():
        # Return a success state. The list of generated resources is left
        # empty. The resource list will be updated by the workflow controller.
        return current_state.success()
    return current_state
