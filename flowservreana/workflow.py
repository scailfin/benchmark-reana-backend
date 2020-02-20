# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Wrapper for workflow templates that follow the syntax of the REANA workflow
specifications.
"""

import flowserv.model.template.parameter as tp


class REANAWorkflow(object):
    """Wrapper around a workflow template for REANA workflow specifications."""
    def __init__(self, template, arguments):
        """Initialize the object properties.

        Parameters
        ----------
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations
        arguments: dict(flowserv.model.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template
        """
        self.template = template
        self.arguments = arguments

    @property
    def output_files(self):
        """Replace references to template parameters in the list of output
        files in the workflow specification.

        Returns
        -------
        list(string)

        Raises
        ------
        flowserv.core.error.InvalidTemplateError
        flowserv.core.error.MissingArgumentError
        """
        workflow_spec = self.template.workflow_spec
        return tp.replace_args(
            spec=workflow_spec.get('outputs', {}).get('files', {}),
            arguments=self.arguments,
            parameters=self.template.parameters
        )

    @property
    def upload_files(self):
        """Get a list of all input files from the workflow specification that
        need to be uploaded for a new workflow run. This is a wrapper around
        the generic get_upload_files function, specific to the workflow
        template syntax that is supported for serial workflows.

        Returns a list of tuples containing the full path to the source file on
        local disk and the relative target path for the uploaded file.

        Raises errors if (i) an unknown parameter is referenced or (ii) if the
        type of a referenced parameter in the input files section is not of
        type file.

        Returns
        -------
        list((string, string))

        Raises
        ------
        flowserv.core.error.InvalidTemplateError
        flowserv.core.error.MissingArgumentError
        flowserv.core.error.UnknownParameterError
        """
        workflow_spec = self.template.workflow_spec
        return tp.get_upload_files(
            template=self.template,
            basedir=self.template.sourcedir,
            files=workflow_spec.get('inputs', {}).get('files', []),
            arguments=self.arguments,
        )

    @property
    def workflow_spec(self):
        """Get expanded workflow specification. Replaces all references to
        template parameters with the respective argument or default values.

        Returns
        -------
        dict()

        Raises
        ------
        flowserv.core.error.InvalidTemplateError
        flowserv.core.error.MissingArgumentError
        """
        # Get the input/parameters dictionary from the workflow specification
        # and replace all references to template parameters with the given
        # arguments or default values.
        return tp.replace_args(
            spec=self.template.workflow_spec,
            arguments=self.arguments,
            parameters=self.template.parameters
        )
