# Database Migration from MongoDB to S3
## Tech Stack

1. Fastapi
2. Pymongo
3. pandas, numpy, other standard libraries

## Usage

Setting up environment
```bash
conda env create -f environment.yml
pip install -r requirements.txt
```

Saving ```.env``` file inside your ```config``` folder
```bash
MONGOPWD=pwd
MONGOUSR=userid
```

Edit ```database.py``` file inside your ```config``` folder by changing the mongoURL

Create a new folder ```converted``` inside the parent directory for saving purposes.

Edit the ```migration_properties.py``` as per requirements. A sample is already present in the file. The file is present inside ```config``` folder

Run the following command from the main directory
```bash
uvicorn main:app --reload
```
