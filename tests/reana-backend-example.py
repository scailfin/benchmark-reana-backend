import os
import shutil
import time

import logging

from benchreana.client import REANAClient
from benchreana.engine import REANAWorkflowEngine
from benchtmpl.io.files.base import FileHandle
from benchtmpl.workflow.template.repo import TemplateRepository

CANCEL_EXAMPLE = False
DATA_FILE = './files/names.txt'
TEMPLATE_FILE = './files/template/template.yaml'
TMP_DIR = './.tmp'
WORKFLOW_DIR = './files/template'

logging.basicConfig(level=logging.INFO)

if os.path.isdir(TMP_DIR):
    shutil.rmtree(TMP_DIR)

engine = REANAWorkflowEngine(
    base_dir=os.path.join(TMP_DIR, 'engine'),
    client=REANAClient()
)
repo = TemplateRepository(base_dir=os.path.join(TMP_DIR, 'repo'))

template = repo.add_template(
    src_dir=WORKFLOW_DIR,
    template_spec_file=TEMPLATE_FILE
)
arguments = {
    'names': template.get_argument('names', FileHandle(DATA_FILE)),
    'sleeptime': template.get_argument('sleeptime', 3)
}

# Run workflow asyncronously
run_id = engine.execute(template, arguments)
state = engine.get_state(run_id)
while state.is_active():
    if CANCEL_EXAMPLE and state.is_running():
        engine.cancel_run(run_id)
        break
    else:
        logging.info('Sleep for 1 sec.')
        time.sleep(1)
        state = engine.get_state(run_id)
state = engine.get_state(run_id)
print(state)
if state.is_error():
    print(state.messages)
