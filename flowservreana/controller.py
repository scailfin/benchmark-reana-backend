# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation for a workflow controller backend that uses an existing REANA
cluster for workflow execution.
"""

from flowserv.controller.remote.engine import RemoteWorkflowController
from flowserv.model.template.base import WorkflowTemplate
from flowservreana.client import REANAClient

import flowserv.core.error as err
import flowserv.model.template.parameter as tp


class REANAWorkflowController(RemoteWorkflowController):
    """Workflow controller that executes workflow templates for a given set of
    arguments using an existing REANA cluster. At this point, each workflow is
    executed as a serial workflow. Each workflow is executed in a separate
    process is the asyc-flag is True.
    """
    def __init__(self, client=None, is_async=None):
        """Initialize the workflow controller. Creates an instance of the REANA
        client that is used to control workflow execution on a REANA cluster.

        Parameters
        ----------
        client: flowservreana.client.REANAClient
            Client to interact with the REANA cluster.
        is_async: bool, optional
            Flag that determines whether workflows execution is synchronous or
            asynchronous by default.
        """
        super().__init__(
            client=client if client is not None else REANAClient(),
            is_async=is_async
        )

    def modify_template(self, template, parameters):
        """Modify a the workflow specification in a given template by adding
        the a set of parameters. If a parameter in the added parameters set
        already exists in the template the name, index, default value, the
        value list and the required flag of the existing parameter are replaced
        by the values of the given parameter.

        Returns a modified workflow template. Raises an error if the parameter
        identifier in the resulting template are no longer unique.

        Note that this currently will only work for serial REANA workflow
        specifications. Will raise an InvalidTemplateError otherwise.

        Parameters
        ----------
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template handle.
        parameters: dict(flowserv.model.parameter.base.TemplateParameter)
            Additional template parameters

        Returns
        -------
        flowserv.model.template.base.WorkflowTemplate

        Raises
        ------
        flowserv.core.error.InvalidTemplateError
        """
        workflow_spec = template.workflow_spec
        # Raise an invalid template error if the workflow specification is not
        # a REANA serial workflow.
        workflow_type = workflow_spec.get('workflow', {}).get('type', 'null')
        if workflow_type != 'serial':
            msg = "invalid workflow type '{}'".format(workflow_type)
            raise err.InvalidTemplateError(msg)
        # Get a copy of the files and parameters sections of the inputs
        # declaration
        inputs = workflow_spec.get('inputs', dict())
        in_files = list(inputs.get('files', list()))
        in_params = dict(inputs.get('parameters', dict()))
        # Ensure that the identifier for all parameters are unique
        para_merge = dict(template.parameters)
        for para in parameters.values():
            if para.identifier in para_merge:
                para = para_merge[para.identifier].merge(para)
            para_merge[para.identifier] = para
            # Depending on whether the type of the parameter is a file or not we
            # add a parameter reference to the respective input section
            if para.is_file():
                in_files.append(tp.VARIABLE(para.identifier))
            else:
                if para.identifier not in in_params:
                    in_params[para.identifier] = tp.VARIABLE(para.identifier)
        spec = dict(workflow_spec)
        spec['inputs'] = {'files': in_files, 'parameters': in_params}
        return WorkflowTemplate(
            workflow_spec=spec,
            sourcedir=template.sourcedir,
            parameters=para_merge,
            modules=template.modules,
            postproc_spec=template.postproc_spec,
            result_schema=template.result_schema
        )
