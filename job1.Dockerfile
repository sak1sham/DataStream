FROM python:3.8

COPY ./config /config
COPY ./db /db
COPY ./dst /dst
COPY ./helper /helper
COPY main.py .
COPY requirements.txt .
RUN python3 -m pip install --upgrade pip
RUN pip3 --no-cache-dir install --upgrade awscli 
RUN pip3 install pip-tools

COPY requirements.in .
RUN pip-sync

# CMD ["python", "main.py","sql:cmdb:leader_kycs"]
CMD ["python","test.py"]
EXPOSE 8080