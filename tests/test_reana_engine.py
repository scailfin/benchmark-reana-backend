"""Test functinality of the REANA backend."""

from unittest import TestCase

import os
import shutil
import time

from benchreana.engine import REANAWorkflowEngine
from benchreana.tests import REANATestClient
from benchtmpl.io.files.base import FileHandle
from benchtmpl.workflow.template.repo import TemplateRepository

import benchtmpl.error as err


DATA_FILE = './tests/files/names.txt'
TEMPLATE_FILE = 'tests/files/template/template.yaml'
TEMPLATE_WITH_INVALID_CMD = 'tests/files/template/template-invalid-cmd.yaml'
TEMPLATE_WITH_MISSING_FILE = 'tests/files/template/template-missing-file.yaml'
TMP_DIR = './tests/files/.tmp'
UNKNOWN_FILE = './tests/files/.tmp/no/file/here'
WORKFLOW_DIR = './tests/files/template'


class TestREANAWorkflowEngine(TestCase):
    """Test executing workflows from templates using the REANA workflow
    engine. Uses the test client that does not require a running REANA cluster
    for unit testing.
    """
    def setUp(self):
        """Create an empty target directory for each test and intitialize the
        file loader function.
        """
        self.tearDown()
        self.engine = REANAWorkflowEngine(
            base_dir=os.path.join(TMP_DIR, 'engine'),
            client=REANATestClient(base_dir=os.path.join(TMP_DIR, 'cluster'))
        )
        self.repo = TemplateRepository(base_dir=os.path.join(TMP_DIR, 'repo'))

    def tearDown(self):
        """Remove the temporary target directory."""
        #if os.path.isdir(TMP_DIR):
        #    shutil.rmtree(TMP_DIR)
        pass

    def test_run_helloworld(self):
        """Execute the helloworld example."""
        template = self.repo.add_template(
            src_dir=WORKFLOW_DIR,
            template_spec_file=TEMPLATE_FILE
        )
        arguments = {
            'names': template.get_argument('names', FileHandle(DATA_FILE)),
            'sleeptime': template.get_argument('sleeptime', 3)
        }
        # Run workflow asyncronously
        run_id = self.engine.execute(template, arguments)
        while self.engine.get_state(run_id).is_active():
            time.sleep(1)
        state = self.engine.get_state(run_id)
        self.assertTrue(state.is_success())
        self.assertEqual(len(state.resources), 1)
        self.assertTrue('results/greetings.txt' in state.resources)
        greetings = list()
        with open(state.resources['results/greetings.txt'].filepath, 'r') as f:
            for line in f:
                greetings.append(line.strip())
        self.assertEqual(len(greetings), 2)
        self.assertEqual(greetings[0], 'Hello Alice!')
        self.assertEqual(greetings[1], 'Hello Bob!')


if __name__ == '__main__':
    import unittest
    unittest.main()
