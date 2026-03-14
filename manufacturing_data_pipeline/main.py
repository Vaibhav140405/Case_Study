"""
Main entry point for the manufacturing data pipeline.

Just run this file to execute the full pipeline:
  python main.py
"""

from pipeline.runner import run_pipeline


def main() -> None:
    """Start the pipeline."""
    run_pipeline()


if __name__ == "__main__":
    main()

