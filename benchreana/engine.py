# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of a workflow engine for parameterized workflow templates
that uses an existing REANA cluster to run workflow instances for given set
of parameter argument values.
"""

import os
import shutil

from datetime import datetime

from benchreana.client import REANAClient
from benchreana.files import FileUploader
from benchtmpl.workflow.resource.base import FileResource

import benchreana.client as rn
import benchtmpl.backend.files as fileio
import benchtmpl.error as err
import benchtmpl.util.core as util
import benchtmpl.workflow.state as wf
import benchtmpl.workflow.template.base as tmpl


"""File and folder names for workflow metadata and downloaded workflow result
files.
"""
OUTPUT_DIR = 'files'
STATE_FILE = 'state.json'


"""Labels for elements materialized workflow state metadata."""
LABEL_OUTPUT_FILES = 'outputs'
LABEL_STATE = 'state'
LABEL_WORKFLOW_ID = 'workflowId'


class REANAWorkflowEngine(object):
    """The workflow engine is used to execute workflow templates for a given
    set of arguments for template parameters as well as to check the state of
    the workflow execution. The REANA engine uses a REANA cluster to execute
    the workflow.

    Workflow executions, referred to as runs, are identified by unique run ids
    that are assigned by the engine when the execution starts.

    Information about the workflow state and result files are maintained in a
    local folder. The folder structure for each run is:

    state.json: JSON file with workflow state and metadata
    files/: Subfolder where result files are downloaded to once they become
        available
    """
    def __init__(self, base_dir, client=None):
        """Initialize the base directory under which all workflow runs are
        maintained. If the directory does not exist it will be created.

        Parameters
        ----------
        base_dir: string
            Path to directory on disk
        client: benchreana.client.REANAClient
            Client to interact with the REANA cluster
        """
        # Set base directory. Create the directory if it does not exist.
        self.base_dir = base_dir
        util.create_dir(self.base_dir)
        # Set the REANA client. If the client is not given as an argument the
        # default client implementation is used.
        if client is None:
            self.reana = REANAClient()
        else:
            self.reana = client

    def cancel_run(self, run_id):
        """Request to cancel execution of the given run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Raises
        ------
        benchreana.error.REANABackendError
        benchtmpl.error.UnknownRunError
        """
        # Get the run directory. Raises error if the directory does not exist
        run_dir = os.path.join(self.base_dir, run_id)
        if not os.path.isdir(run_dir):
            raise err.UnknownRunError(run_id)
        # Read materialized workflow state from file to get the REANA workflow
        # identifier
        filename = os.path.join(run_dir, STATE_FILE)
        obj = util.read_object(filename)
        state = wf.WorkflowState.from_dict(obj[LABEL_STATE])
        # Nothing to do if the workflow is inactive
        if not state.is_active():
            return state
        elif state.is_running():
            # The REANA cluster only allows running workflows to be stopped.
            workflow_id = obj[LABEL_WORKFLOW_ID]
            self.reana.stop_workflow(workflow_id)
        # Update the workflow state to is_error
        state = state.error(messages=['canceled by user'])
        # Write updated state to file
        obj[LABEL_STATE] = state.to_dict()
        util.write_object(filename=filename, obj=obj)

    def execute(self, template, arguments):
        """Execute a given workflow template for a set of argument values.
        Create a REANA workflow specification from the template and the given
        agument values. The specification is submitted to the REANA cluster to
        create the workflow. The second step is to upload all required files
        to the REANA cluster. After file upload the workflow is started on the
        remote cluster.

        Returns the unique workflow identifier for the started workflow run as
        returned by the REANA cluster.

        Parameters
        ----------
        template: benchtmpl.workflow.template.base.TemplateHandle
            Workflow template containing the parameterized specification and the
            parameter declarations
        arguments: dict(benchtmpl.workflow.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Returns
        -------
        string

        Raises
        ------
        benchreana.error.REANABackendError
        benchtmpl.error.MissingArgumentError
        """
        # Before we start creating directories and copying files make sure that
        # there are values for all template parameters (either in the arguments
        # dictionary or set as default values)
        template.validate_arguments(arguments)
        # Create unique run identifier. The identifier is different from the
        # workflow identifier that REANA genereates.
        identifier = util.get_unique_identifier()
        # Create run folder for state file and create subfolder for downloaded
        # result files
        run_dir = os.path.join(self.base_dir, identifier)
        os.makedirs(run_dir)
        os.makedirs(os.path.join(run_dir, OUTPUT_DIR))
        # Copy all static files and files in the argument dictionary into the
        # run folder.
        try:
            # Create the REANA workflow specification from the template and the
            # given argument values
            workflow_spec = tmpl.replace_args(
                spec=template.workflow_spec,
                arguments=arguments,
                parameters=template.parameters
            )
            # Create a workflow on the REANA cluster.
            response = self.reana.create_workflow(workflow_spec)
            # Get the unique identifier for the created workflow from the
            # response
            workflow_id = response['workflow_id']
            # Upload all required code and input files to the workspace of the
            # created workflow
            fileio.upload_files(
                template=template,
                files=template.workflow_spec.get('inputs', {}).get('files', []),
                arguments=arguments,
                loader=FileUploader(workflow_id, client=self.reana)
            )
            # Run workflow. At this point we materialize the workflow state as
            # created. Successive calls to the get_state method will update the
            # workflow state accordingly
            self.reana.start_workflow(workflow_id)
            state = wf.StatePending(created_at=datetime.now())
            output_files = workflow_spec.get('outputs', {}).get('files', {})
            filename = os.path.join(run_dir, STATE_FILE)
            obj = {
                LABEL_WORKFLOW_ID: workflow_id,
                LABEL_OUTPUT_FILES: output_files,
                LABEL_STATE: state.to_dict()
            }
            util.write_object(filename=filename, obj=obj)
        except Exception as ex:
            # Remove run directory if anything goes wrong while preparing the
            # workflow and starting the run
            shutil.rmtree(run_dir)
            raise ex
        # Return run identifier
        return identifier

    def get_state(self, run_id):
        """Get the status of the workflow with the given identifier. This
        method will query the REANA cluster for the current workflow state only
        if the local copy of the state signals that the workflow is active. If
        the state returned by the cluster differs from the local copy the local
        metadata copy is updated accordingly.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        benchtmpl.workflow.state.WorkflowState

        Raises
        ------
        benchreana.error.REANABackendError
        benchtmpl.error.UnknownRunError
        """
        # Get the run directory. Raises error if the directory does not exist
        run_dir = os.path.join(self.base_dir, run_id)
        if not os.path.isdir(run_dir):
            raise err.UnknownRunError(run_id)
        # Read materialized workflow state from file
        filename = os.path.join(run_dir, STATE_FILE)
        obj = util.read_object(filename)
        state = wf.WorkflowState.from_dict(obj[LABEL_STATE])
        # Nothing to do if the workflow is inactive
        if not state.is_active():
            return state
        # For active workflows we need to get the current state from the REANA
        # cluster
        workflow_id = obj[LABEL_WORKFLOW_ID]
        response = self.reana.get_current_status(workflow_id)
        reana_status = response.get('status')
        ts = datetime.now()
        if state.is_running():
            started_at = state.started_at
        else:
            started_at = ts
        state_change = False
        if reana_status in rn.REANA_STATE_RUNNING and not state.is_running():
            state = wf.StateRunning(
                created_at=state.created_at,
                started_at=started_at
            )
            state_change = True
        elif reana_status in rn.REANA_STATE_ERROR:
            state = wf.StateError(
                created_at=state.created_at,
                started_at=started_at,
                stopped_at=ts,
                messages=response['logs']
            )
            state_change = True
        elif reana_status in rn.REANA_STATE_SUCCESS:
            # Download result files to local files folder
            resources = dict()
            files_dir = os.path.join(run_dir, OUTPUT_DIR)
            for source in obj[LABEL_OUTPUT_FILES]:
                target = os.path.join(files_dir, source)
                self.reana.download_file(workflow_id, source, target)
                res = FileResource(identifier=source, filepath=target)
                resources[source] = res
            state = wf.StateSuccess(
                created_at=state.created_at,
                started_at=started_at,
                finished_at=ts,
                resources=resources
            )
            state_change = True
        # Write updated state to file (if changed)
        if state_change:
            obj[LABEL_STATE] = state.to_dict()
            util.write_object(filename=filename, obj=obj)
        return state
