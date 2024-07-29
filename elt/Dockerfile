FROM python:3.10-slim

RUN apt-get update && apt-get install -y wget gnupg2

RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update

RUN apt-get install -y postgresql-client-16

COPY elt_script.py .

CMD ["python", "elt_script.py"]
