# Recharge Connector

A Python package for interacting with the Recharge Payments API, specifically focused on subscription data retrieval.

## Installation

```bash
pip install recharge-connector
```

## Requirements

- Python 3.12+
- Required packages:
  - polars >= 1.27.1
  - python-dotenv >= 1.1.0
  - requests >= 2.32.3
  - tqdm >= 4.67.1

## Setup

1. Create a `.env` file in your project root with your Recharge API credentials:

```
staging_api_token=your_staging_token_here
prod_api_token=your_production_token_here
```

## Usage

```python
from recharge_connector import pull_active_subs

# Fetch all active subscriptions
subscriptions_df = pull_active_subs()
```

The `pull_active_subs()` function returns a Polars DataFrame containing active subscription data with the following transformations:
- Converted variant and product IDs to proper numeric types
- Price fields cast to float
- Unnecessary fields removed
- Nested JSON structures unnested

## Features

- Automatic pagination handling
- Rate limiting protection (0.5s delay between requests)
- Progress bar for long-running requests
- Data cleaning and type conversion
- Error handling for API requests

## Development

To set up the development environment:

```bash
python -m venv .venv
.venv\Scripts\activate  # On Windows
# source .venv/bin/activate  # On Linux/MacOS
pip install -e .
```

### Using UV (Optional)

For faster dependency resolution and installation, you can use [uv](https://github.com/astral-sh/uv):

#### Windows Installation
```powershell
# Install with PowerShell
(Invoke-WebRequest -Uri https://github.com/astral-sh/uv/releases/latest/download/uv-windows-x64.zip -OutFile uv.zip)
Expand-Archive -Path uv.zip -DestinationPath $env:LOCALAPPDATA\uv
$oldPath = [Environment]::GetEnvironmentVariable('Path', 'User')
$uvPath = "$env:LOCALAPPDATA\uv"
[Environment]::SetEnvironmentVariable('Path', "$oldPath;$uvPath", 'User')
```

#### Linux/MacOS Installation
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

#### Using UV
```bash
# Create virtual environment and install dependencies
uv venv
.venv\Scripts\activate  # On Windows
# source .venv/bin/activate  # On Linux/MacOS
uv pip install -e .
```

## Author

Daniel Ross (daniel.ross@aop.com)