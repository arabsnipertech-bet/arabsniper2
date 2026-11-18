"""Build an ephemeral Workers bundle from the exact GitHub Publisher sources."""
import ast
import base64
import json
from pathlib import Path
import shutil
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
src = ROOT / 'src'
out = Path(sys.argv[1])
shutil.copytree(HERE / 'renderer', out, dirs_exist_ok=True)
pkg = out / 'publisher_v71'
pkg.mkdir(exist_ok=True)
for name in ('__init__.py', 'editorial.py', 'live_events.py'):
    shutil.copyfile(src / 'publisher_v71' / name, pkg / name)
shutil.copyfile(src / 'telegram_live_art.py', out / 'telegram_live_art.py')
fonts = Path('/usr/share/fonts/truetype/dejavu')
names = ('DejaVuSans.ttf','DejaVuSans-Bold.ttf','DejaVuSerif.ttf')
assets = {str(fonts / name):base64.b64encode((fonts / name).read_bytes()).decode() for name in names}
rome = base64.b64encode(Path('/usr/share/zoneinfo/Europe/Rome').read_bytes()).decode()
(pkg / 'selection.py').write_text(
    'from zoneinfo import ZoneInfo\nfrom io import BytesIO\nfrom base64 import b64decode\n'
    + f"ROME = ZoneInfo.from_file(BytesIO(b64decode({rome!r})), key='Europe/Rome')\n")
model = (src / 'multimarket_model.py').read_text()
stamp = next(node for node in ast.parse(model).body if getattr(node,'name',None) == 'stamp')
(out / 'multimarket_model.py').write_text('from datetime import datetime\n' + ast.get_source_segment(model,stamp) + '\n')
# The original renderers open absolute Ubuntu font paths. Materialize the same bytes in the worker VFS.
bootstrap = ('from pathlib import Path\nfrom base64 import b64decode\n'
             + 'for name, data in ' + repr(assets) + '.items():\n'
             + '    path = Path(name)\n    path.parent.mkdir(exist_ok=True, parents=True)\n'
             + '    path.write_bytes(b64decode(data))\n')
# The runtime may mount the root read-only. Use /tmp, preserving font filenames.
bootstrap = bootstrap.replace('path = Path(name)', "path = Path('/tmp/shadow-fonts') / Path(name).name")
(out / 'shadow_fonts.py').write_text(bootstrap)
for path in (pkg / 'editorial.py', out / 'telegram_live_art.py'):
    path.write_text(path.read_text().replace(str(fonts), '/tmp/shadow-fonts'))
entry = out / 'entry.py'
entry.write_text('import shadow_fonts\n' + entry.read_text())
