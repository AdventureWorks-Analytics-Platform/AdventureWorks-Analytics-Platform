# Main entry point for AdventureWorks Analytics Platform

"""Entry point for the platform. This file is intentionally thin and delegates
real orchestration to the application layer."""

from src.app.app import App
from src.app.cli import main as cli_main


def run_application():
    """Run the application and preserve the dictionary result API."""
    app = App()
    return app.run()


def main(argv=None):
    """Run the process-facing CLI and return its exit code."""
    return cli_main(argv=argv, app_factory=App)


if __name__ == "__main__":
    raise SystemExit(main())
