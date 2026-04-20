# Vinted Discord Alerts

Web UI for multi-geo Vinted search plus Discord webhook watchers for new listings.

## Start

```powershell
cd D:\par
python -m pip install -r requirements.txt
python .\app.py
```

Open:

```text
http://127.0.0.1:5000
```

## What it does

- search by brand or item
- choose geos and page count
- filter by price
- export JSON and CSV
- save a watcher with one Discord webhook URL
- poll in the background and alert only on new unseen listings

## Console mode

```powershell
python .\vinted_parser.py --query "nike tech fleece" --geo fr,de,it --pages 2
```
