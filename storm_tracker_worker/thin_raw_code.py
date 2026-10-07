"""Content identity of packaged code/migrations, excluding private configuration."""
import hashlib
from pathlib import Path


def source_tree_digest(root=None):
    root = Path(__file__).parent if root is None else Path(root)
    digest = hashlib.sha256()
    for path in sorted(root.rglob('*')):
        if not path.is_file() or path.suffix not in {'.py', '.sql'}:
            continue
        relative = path.relative_to(root)
        if '__pycache__' in relative.parts:
            continue
        body = path.read_bytes()
        name = relative.as_posix().encode()
        digest.update(len(name).to_bytes(8,'big'))
        digest.update(name)
        digest.update(len(body).to_bytes(8,'big'))
        digest.update(body)
    return digest.hexdigest()
