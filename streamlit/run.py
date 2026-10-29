"""Start the dashboard with: python streamlit/run.py."""

from pathlib import Path
import subprocess


def main():
    root = Path(__file__).resolve().parent.parent
    return subprocess.call(
        [
            'poetry',
            'run',
            'streamlit',
            'run',
            str(root / 'streamlit' / 'app.py'),
            '--server.address',
            '127.0.0.1',
            '--server.port',
            '8501',
        ],
        cwd=root,
    )


if __name__ == '__main__':
    raise SystemExit(main())
