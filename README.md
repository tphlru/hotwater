# Hotwater Summer

This is a Python package for charting and data tools for the Hotwater project.

## Installation (editable mode)

From the root of the repository, run:

```bash
pip install -e ./hotwater/summer
```

## Usage

`export QT_QPA_PLATFORM=xcb`

You can now import modules from anywhere in the project using absolute imports, e.g.:

```python
from data.dataloader import load_configs
```

## Structure

- `data/` — data loading and config
- `factors/` — analysis modules
- `interface/` — UI and charting

---

Add more usage and development notes here.
