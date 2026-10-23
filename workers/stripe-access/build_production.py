"""Build the standalone production Worker for Cloudflare's editor."""
from pathlib import Path
root = Path(__file__).resolve().parent
auth = (root / 'auth.mjs').read_text()
worker = (root / 'worker.mjs').read_text()
import_line = "import {authRoute, cleanupAuth} from './auth.mjs';"
if worker.count(import_line) != 1:
    raise ValueError('Unexpected auth import; inspect bundle generation')
bundle = ("// Production bundle: generated from worker.mjs and auth.mjs. Keep AUTH_ENABLED=false until cutover.\n"
          "const {authRoute, cleanupAuth} = (() => {\n"
          + auth.replace('export ', '')
          + "\nreturn {authRoute,cleanupAuth};\n})();\n"
          + worker.replace(import_line, ''))
(root / 'production-bundle.mjs').write_text(bundle)

