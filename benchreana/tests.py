# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test client for the REANA backend. This client is used to run unittests
without having access to a REANA cluster.
"""

import errno
import os
import shutil

from string import Template

from benchproc.backend.engine import MultiProcessWorkflowEngine
from benchreana.error import REANABackendError
from benchtmpl.backend.files import FileCopy

import benchreana.client as rn
import benchtmpl.util.core as util


class REANATestClient(object):
    """Implementation of the REANA client for test purposes. Uses the multi-
    process backend to execute workflows.
    """
    def __init__(self, base_dir):
        """Initialize the multi-process backend that is used for workflow
        execution.

        Parameters
        ----------
        base_dir: string
            Path to the directory that where the multi-process backend stores
            workflow run information
        """
        self.engine = MultiProcessWorkflowEngine(base_dir)
        self.base_dir = base_dir

    def create_workflow(self, reana_specification):
        """Create a new instance of a workflow from the given workflow
        specification. The test implementation writes the workflow specification
        to file for further references.

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
        # Remove all files in the base directory
        for filename in os.listdir(self.base_dir):
            file_path = os.path.join(self.base_dir, filename)
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        # Get unique workflow identifier and write workflow specification to
        # disk for further references
        identifier = util.get_unique_identifier()
        filename = os.path.join(self.base_dir, identifier + '.json')
        util.write_object(filename, reana_specification)
        return {
            'message': rn.REANA_STATE_PENDING,
            'workflow_id': identifier,
            'workflow_name': 'workflow.1'
        }

    def download_file(self, workflow_id, source, target):
        """Download file from relative location at REANA cluster to target path.

        Parameters
        ----------
        source: string
            Relative path to source file in workflow workspace at REANA cluster
        target: string
            Path to target file on local disk
        """
        filename = os.path.join(self.base_dir, source)
        try:
            shutil.copy(src=filename, dst=target)
        except IOError as e:
            # ENOENT(2): file does not exist, raised also on missing dest
            # parent dir
            if e.errno != errno.ENOENT or not os.path.isfile(filename):
                raise
            # try creating parent directories
            os.makedirs(os.path.dirname(target))
            shutil.copy(src=filename, dst=target)

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
        """
        state = self.engine.get_state(workflow_id)
        obj = {
            'created': state.created_at.isoformat(),
            'id': workflow_id,
            'name': 'workflow.1',
            'progress': state.type_id
        }
        if state.is_pending():
            obj['status'] = rn.REANA_STATE_PENDING
        elif state.is_running():
            obj['status'] = rn.REANA_STATE_RUNNING
        elif state.is_error():
            obj['status'] = rn.REANA_STATE_ERROR
            obj['logs'] = '\n'.join(state.messages)
        elif state.is_success():
            obj['status'] = rn.REANA_STATE_SUCCESS
        return obj

    def start_workflow(self, workflow_id):
        """Execute the workflow with the given identifier.

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
        # Read workflow template from disk
        filename = os.path.join(self.base_dir, workflow_id + '.json')
        spec = util.read_object(filename)
        # Extract list of commands from workflow specification
        params = spec.get('inputs', {}).get('parameters', {})
        commands = list()
        steps = spec.get('workflow', {}).get('specification', {}).get('steps', [])
        for step in steps:
            for command in step.get('commands', []):
                try:
                    commands.append(Template(command).substitute(params))
                except KeyError as ex:
                    raise err.InvalidTemplateError(str(ex))
        # Get list of output files
        state = self.engine.run_async(
            identifier=workflow_id,
            commands=commands,
            run_dir=self.base_dir,
            output_files=spec.get('outputs', {}).get('files', {}),
            verbose=False
        )
        # Translate workflow state into REANA status
        if state.is_running():
            status = rn.REANA_STATE_RUNNING
        elif state.is_error():
            status = rn.REANA_STATE_ERROR
        elif state.is_success():
            status = rn.REANA_STATE_SUCCESS
        else:
            status = rn.REANA_STATE_PENDING
        return {
            'message': state.type_id,
            'status': status,
            'user': '00000000000000',
            'workflow_id': workflow_id,
            'workflow_name': 'workflow.1'
        }

    def upload_file(self, workflow_id, source, target):
        """Upload a local source file to the target location in the workflow
        workspace on the REANA cluster. For this test implementation all files
        are copied into the base directory.

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
        FileCopy(self.base_dir)(source, target)
