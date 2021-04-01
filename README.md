# sayari-spark-problem

## Setup
Create a virtual environment:
```
python -m venv venv
```

Install the requirements:
```
python -m pip install -r requirements.txt
```

Run the PySpark code:
```
spark-submit sayari_spark_project.py
```

## Entity Matching
Matches were found in three ways:
1. Entities in OFAC and UK matched by checking if the name and type are equal, and there is a common reported date of birth.
2. Entities in OFAC and UK matched by checking if there is a common ID number.
3. Entities in OFAC and UK matched by checking if the OFAC name exists in a UK row's alias values.
4. Entities in OFAC and UK matched by checking if the UK name exists in an OFAC row's alias values.