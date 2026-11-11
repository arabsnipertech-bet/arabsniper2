"""Build the standalone production Worker for Cloudflare's editor."""
from pathlib import Path
root = Path(__file__).resolve().parent
auth = (root / 'auth.mjs').read_text()
worker = (root / 'worker.mjs').read_text()
trial = (root / 'telegram-trial.mjs').read_text()
test_login = (root / 'test-login.mjs').read_text()
renewal = (root / 'renewal.mjs').read_text()
renewal_import = "import {renewalEnabled, cachedAccess, invalidateAccessCache, renewalStatus, renewalCheckout, renewalOrder, markRenewalPaid, closeFailedRenewal} from './renewal.mjs';"
import_line = "import {authRoute, cleanupAuth} from './auth.mjs';"
trial_import = "import {telegramTrialRoute} from './telegram-trial.mjs';"
test_import = "import {goldTestLoginPage} from './test-login.mjs';"
if worker.count(import_line) != 1 or worker.count(trial_import) != 1 or worker.count(test_import) != 1 or worker.count(renewal_import) != 1:
    raise ValueError('Unexpected auth import; inspect bundle generation')
bundle = ("// Production bundle: generated from worker.mjs and auth.mjs. Keep AUTH_ENABLED=false until cutover.\n"
          "const {authRoute, cleanupAuth} = (() => {\n"
          + auth.replace('export ', '')
          + "\nreturn {authRoute,cleanupAuth};\n})();\n"
          + "\nconst telegramTrialRoute = (() => {\n"
          + trial.replace('export ', '')
          + "\nreturn telegramTrialRoute;\n})();\n"
          + "\nconst goldTestLoginPage = (() => {\n"
          + test_login.replace('export ', '')
          + "\nreturn goldTestLoginPage;\n})();\n"
          + "\nconst {renewalEnabled, cachedAccess, invalidateAccessCache, renewalStatus, renewalCheckout, renewalOrder, markRenewalPaid, closeFailedRenewal} = (() => {\n"
          + renewal.replace('export ', '')
          + "\nreturn {renewalEnabled,cachedAccess,invalidateAccessCache,renewalStatus,renewalCheckout,renewalOrder,markRenewalPaid,closeFailedRenewal};\n})();\n"
          + worker.replace(renewal_import, '').replace(import_line, '').replace(trial_import, '').replace(test_import, ''))
(root / 'production-bundle.mjs').write_text(bundle)

