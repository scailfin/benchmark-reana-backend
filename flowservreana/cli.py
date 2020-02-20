# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface for the REANA workflow controller. This interface is
primarily intended for testing purposes. It uses the packages' own REANA client
instead of the reana-client to create, start, monitor, and stop REANA
workflows.
"""

import click
import json

from flowserv.model.template.base import WorkflowTemplate
from flowservreana.client import REANAClient
from flowservreana.workflow import REANAWorkflow

import flowserv.core.util as util
import flowservreana.error as err


@click.group()
def cli():
    """Command Line Interface for the Reproducible Open Benchmark REANA
    workflow controller."""
    pass


# -- Create Workflow ----------------------------------------------------------

@cli.command(name='create')
@click.option(
    '-s', '--spec',
    required=True,
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help='Workflow specification file.'
)
def create_workflow(spec):
    """Create a new workflow from the given specification."""
    doc = util.read_object(filename=spec)
    r = REANAClient().create_workflow(doc)
    click.echo(json.dumps(r, indent=4))


# -- Cancel Workflow ----------------------------------------------------------

@cli.command(name='cancel')
@click.option(
    '-w', '--workflow',
    required=True,
    help='Workflow identifier.'
)
def cancel_workflow(workflow):
    """Cancel workflow execution."""
    try:
        r = REANAClient().stop_workflow(workflow)
        click.echo(json.dumps(r, indent=4))
    except err.REANABackendError as ex:
        click.echo('Error: {}'.format(ex))


# -- Start Workflow -----------------------------------------------------------

@cli.command(name='start')
@click.option(
    '-w', '--workflow',
    required=True,
    help='Workflow identifier.'
)
def start_workflow(workflow):
    """Start a workflow run."""
    try:
        r = REANAClient().start_workflow(workflow)
        click.echo(json.dumps(r, indent=4))
    except err.REANABackendError as ex:
        click.echo('Error: {}'.format(ex))


# -- Workflow Status ----------------------------------------------------------

@cli.command(name='status')
@click.option(
    '-w', '--workflow',
    required=True,
    help='Workflow identifier.'
)
def workflow_state(workflow):
    """Get workflow state."""
    try:
        r = REANAClient().get_current_status(workflow)
        click.echo(json.dumps(r, indent=4))
    except err.REANABackendError as ex:
        click.echo('Error: {}'.format(ex))


# -- Upload Workflow Files ----------------------------------------------------

@cli.command(name='upload')
@click.option(
    '-s', '--spec',
    required=True,
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help='Workflow specification file.'
)
@click.option(
    '-d', '--dir',
    required=True,
    type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True),
    help='Base directory for workflow files.'
)
@click.option(
    '-w', '--workflow',
    required=True,
    help='Workflow identifier.'
)
def upload_files(spec, dir, workflow):
    """Upload workflow files."""
    doc = util.read_object(filename=spec)
    template = WorkflowTemplate(workflow_spec=doc, sourcedir=dir)
    wf = REANAWorkflow(template=template, arguments=dict())
    client = REANAClient()
    for source, target in wf.upload_files():
        click.echo('upload {} as {}'.format(source, target))
        r = client.upload_file(workflow, source, target)
        click.echo(json.dumps(r, indent=4))
