name: Run Auditor

on:
  workflow_dispatch:
  schedule:
    - cron: '7 9 * * *'
      timezone: 'Europe/Rome'

permissions:
  contents: write

concurrency:
  group: arabsniper-run-auditor
  cancel-in-progress: false

jobs:
  auditor:
    runs-on: ubuntu-latest
    timeout-minutes: 60

    env:
      API_SPORTS_KEY: ${{ secrets.API_SPORTS_KEY }}

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install requests

      - name: Run audit script
        run: |
          python auditor/run_audit.py

      - name: Commit and push audit results
        run: |
          git config user.name "github-actions"
          git config user.email "github-actions@github.com"
          git add auditarchive/
          git commit -m "Run auditor" || echo "No changes to commit"
          git push
