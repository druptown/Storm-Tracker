"""Blocked historical worker-only deploy route.

Use ``install_ha_release_pair_ssh.py`` with verified pair evidence.  The
current HA source paths are root-owned; this former script wrote them directly,
did not create the required paired rollback run and must not be revived.
"""
from __future__ import annotations


def main() -> None:
    raise SystemExit(
        "Geblokkeerde historische deployroute. Gebruik "
        "scripts/install_ha_release_pair_ssh.py met het geverifieerde "
        "release-evidence en volg docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md."
    )


if __name__ == "__main__":
    main()
