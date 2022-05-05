FROM python:3.9-slim-buster

#Do not cache Python packages
ENV PIP_NO_CACHE_DIR=yes

#Keeeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# set PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/code/"

# Initializing new working directory
WORKDIR /code
 
# Transferring the code and essential data
COPY xetra ./xetra
COPY configs ./configs
COPY Pipfile ./Pipfile
COPY Pipfile.lock ./Pipfile.lock
COPY run.py ./run.py
 
RUN pip install pipenv
RUN pipenv install --ignore-pipfile --system
CMD [ "python", "./run.py"]