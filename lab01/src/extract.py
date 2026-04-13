import csv


def read_csv(path: str):
    """Read CSV file and yield rows as dicts."""
    try:
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path}")