"""Совместимая точка входа исторического grouped-history прототипа."""

from .legacy.repack import expand, main, repack, state_key

__all__ = ["expand", "main", "repack", "state_key"]

if __name__ == "__main__":
    main()
