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
import os

from flowserv.model.run.base import RunHandle
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.workflow.state import StatePending
from flowservreana.client import REANAClient
from flowservreana.workflow import REANAWorkflow

import flowserv.core.util as util


@click.group()
def cli():
    """Command Line Interface for the Reproducible Open Benchmark REANA
    workflow controller."""
    pass


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
        REANAClient().stop_workflow(workflow)
        click.echo('workflow stopped')
    except Exception as ex:
        click.echo('Error: {}'.format(ex))


# -- Create and run workflow --------------------------------------------------

@cli.command(name='run')
@click.option(
    '-s', '--spec',
    required=True,
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help='Workflow specification file.'
)
def run_workflow(spec):
    """Create a new workflow run for the given specification."""
    doc = util.read_object(filename=spec)
    rundir = os.path.dirname(spec)
    if not rundir:
        rundir = '.'
    run = RunHandle(
        identifier='0000',
        workflow_id='0000',
        group_id='0000',
        state=StatePending(),
        arguments=dict(),
        rundir=rundir
    )
    template = WorkflowTemplate(workflow_spec=doc, sourcedir=rundir)
    wf = REANAClient().create_workflow(run, template, dict())
    click.echo('created workflow {} ({})'.format(wf.identifier, wf.state))


# -- Download result file -----------------------------------------------------

@cli.command(name='download')
@click.option(
    '-w', '--workflow',
    required=True,
    help='Workflow identifier.'
)
@click.option(
    '-i', '--source',
    required=True,
    help='Source file'
)
@click.option(
    '-o', '--target',
    required=True,
    help='Target file'
)
def download_file(workflow, source, target):
    """Download file from workflow workspace."""
    REANAClient().download_file(workflow, source, target)


# -- Workflow Status ----------------------------------------------------------

@cli.command(name='status')
@click.option(
    '-w', '--workflow',
    required=True,
    help='Workflow identifier.'
)
def get_workflow_state(workflow):
    """Get workflow state."""
    try:
        state = REANAClient().get_workflow_state(workflow, StatePending())
        click.echo('in state {}'.format(state))
    except Exception as ex:
        click.echo('Error: {}'.format(ex))
