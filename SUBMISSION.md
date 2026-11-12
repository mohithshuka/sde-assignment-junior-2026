# Submission

## Candidate
- **Name:** Mohith Shuka
- **Assignment chosen:** A (Data)
- **Time spent (honest estimate):** ~3h

## How to run
```bash
pip install -r requirements.txt
python -m solution.pipeline ingest data/drop_1
python -m solution.pipeline ingest data/drop_2
python -m solution.pipeline report --out reports
pytest -q