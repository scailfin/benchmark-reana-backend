=========================================================================================
REANA Workflow Controller for the Reproducible and Reusable Data Analysis Workflow Server
=========================================================================================

.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
   :target: https://github.com/scailfin/flowserv-reana-backend/blob/master/LICENSE

.. image:: https://github.com/scailfin/flowserv-reana-backend/workflows/build/badge.svg
   :target: https://github.com/scailfin/flowserv-reana-backend/actions?query=workflow%3A%22build%22



About
=====

This repository contains the implementation of a workflow controller for the `Reproducible and Reusable Data Analysis Workflow Server (flowServ) <https://github.com/scailfin/flowserv-core>`_ that uses an existing `REANA <http://www.reanahub.io/>`_ cluster to run instances of workflow templates.


Configuration
=============

Install the REANA backend package:

.. code-block:: bash

    pip install git+https://github.com/scailfin/flowserv-reana-backend.git


Set the following environment variables to instruct flowServ to use the REANA workflow controller:

.. code-block:: bash

    export FLOWSERV_BACKEND_CLASS=REANAWorkflowController
    export FLOWSERV_BACKEND_MODULE=flowservreana.controller

You also need to set the REANA environment variables **REANA_SERVER_URL** and **REANA_ACCESS_TOKEN**.
