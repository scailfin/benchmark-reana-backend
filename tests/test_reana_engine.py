# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the REANA workflow controller and client."""

import os
import time

from flowserv.service.api import API
from flowserv.tests.files import FakeStream
from flowservreana.client import REANAClient
from flowservreana.controller import REANAWorkflowController
from flowservreana.tests import REANATestAPI

import flowserv.config.api as config
import flowserv.core.util as util
import flowserv.model.workflow.state as st
import flowserv.tests.db as db


# Template directory
DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, './.files')


# Default users
UID = '0000'


def test_run_reana_workflow(tmpdir):
    """Execute the fake workflow example."""
    # -- Setup ----------------------------------------------------------------
    # Create the database and service API with a serial workflow engine in
    # asynchronous mode
    os.environ[config.FLOWSERV_API_BASEDIR] = os.path.abspath(str(tmpdir))
    api = API(
        con=db.init_db(str(tmpdir), users=[UID]).connect(),
        engine=REANAWorkflowController(
            client=REANAClient(
                reana_client=REANATestAPI(basedir=str(tmpdir)),
                access_token='XXXX'
            )
        )
    )
    # Create workflow template and run group
    wh = api.workflows().create_workflow(name='W1', sourcedir=TEMPLATE_DIR)
    w_id = wh['id']
    gh = api.groups().create_group(workflow_id=w_id, name='G', user_id=UID)
    g_id = gh['id']
    # Upload the names file
    fh = api.uploads().upload_file(
        group_id=g_id,
        file=FakeStream(data={'action': 'SUCCESS'}),
        name='sucess.txt',
        user_id=UID
    )
    file_id = fh['id']
    # -- Test successful run --------------------------------------------------
    # Run the workflow
    run = api.runs().start_run(
        group_id=g_id,
        arguments=[{'id': 'todo', 'value': file_id}],
        user_id=UID
    )
    r_id = run['id']
    # Poll workflow state every second.
    while run['state'] in st.ACTIVE_STATES:
        time.sleep(1)
        run = api.runs().get_run(run_id=r_id, user_id=UID)
    assert run['state'] == st.STATE_SUCCESS
    resources = dict()
    for r in run['resources']:
        resources[r['name']] = r['id']
    f_id = resources['results/outputs.json']
    fh = api.runs().get_result_file(run_id=r_id, resource_id=f_id, user_id=UID)
    doc = util.read_object(fh.filename)
    assert doc['action'] == 'SUCCESS'
    # -- Test error run --------------------------------------------------
    # Upload error file
    fh = api.uploads().upload_file(
        group_id=g_id,
        file=FakeStream(data={'action': 'ERROR'}),
        name='error.txt',
        user_id=UID
    )
    file_id = fh['id']
    # Run the workflow
    run = api.runs().start_run(
        group_id=g_id,
        arguments=[{'id': 'todo', 'value': file_id}],
        user_id=UID
    )
    r_id = run['id']
    # Poll workflow state every second.
    while run['state'] in st.ACTIVE_STATES:
        time.sleep(1)
        run = api.runs().get_run(run_id=r_id, user_id=UID)
    assert run['state'] == st.STATE_ERROR
    assert run['messages'] == ['unknown reason']
