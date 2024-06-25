# Telegram Jobs Parser Module

`tg_jobs_parser_module` is a Python module for automatically gathering and processing job vacancy information from Telegram channels. 
[Example of ELT pipeline by Airflow.](https://github.com/WuDMC/ELT_DAGs_for_tg_jobs_parser)

## Features

- Parsing job postings from Telegram channels
- Data processing and filtering
- Google Cloud integration

## Installation

1. **Clone the repository:**

    ```bash
    git clone https://github.com/WuDMC/tg_jobs_parser_module.git
    cd tg_jobs_parser_module/tg_jobs_parser
    ```

2. **Install the dependencies:**

    ```bash
      make install
    ```

## Usage

1. **Configure the parameters in `configs/vars` and .env** 

    - `api_id`, `api_hash`, and `phone` can be obtained by registering your app at [my.telegram.org](https://my.telegram.org).
    - `credentials` should point to your Google Cloud credentials JSON file.


## Project Structure

- **`configs/`** - Configuration files.
- **`google_cloud_helper/`** - Modules for Google Cloud integration.
- **`telegram_helper/`** - Modules for interacting with Telegram.
- **`tests/`** - Unit tests for the modules.
- **`utils/`** - Utility modules.
- **`__init__.py`** - Initializes the Python package.

## Configuration

Example configuration

todo later

