import sys
from pathlib import Path

root = str(Path.cwd())
if root not in sys.path:
    sys.path.insert(0, root)
# print(sys.path)

false, true = False, True
