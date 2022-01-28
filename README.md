# Database Migration from MongoDB to S3
## Tech Stack
Python version 3.9.1

1. Fastapi
2. Pymongo
3. pandas, numpy, other standard libraries

## Usage

Setting up environment
```bash
pip install -r requirements.txt
```

Create a new folder ```converted``` inside the parent directory for saving purposes.

Edit the ```/config/migration_mapping.py``` as per requirements.

Run the following command from the main directory
```bash
python main.py
```
